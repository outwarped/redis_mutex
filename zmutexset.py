from __future__ import absolute_import
from time import sleep
import redis
import logging
from datetime import datetime
from typing import Optional, Tuple, Collection, Collection
import threading

logger = logging.getLogger(__name__)


class ZMutexSet(object):
    """Named Mutexes implementation with Redis Sorted List.         
    """
    def __init__(self, redis:redis.client.Redis, zsetname="zmutex", timeout_seconds:int=60, revalidate_seconds:int=1):
        """Named Mutexes implementation with Redis Sorted List.         
        * Semaphore is regarded "expired" timeout_seconds after calling acquire() or last updated().
        * Expired mutexes can be reacquired with acquire(). Therefore crashed clients will release their mutexes.
        * When actor fails to update mutexes in before it expires and another actor reacquires this semaphore, 
        all operations of this semaphore by the first actor will fail. Technically semaphore update()/release() fails if 
        zscore (timestamp) returned by previous acquire()/update() doesnt match current semaphore zscore.
        * Expired mutexes cannot be update()/release()
        * Expired mutexes recalculated in every acquire()/update()/release() and every revalidate_seconds in blocking acquire().
        * create() mutexes are marked "expired"
        * release() mutexes are deleted or marked "expired"

        Args:
            redis (redis.client.Redis): Redis client object
            zsetname (str, optional): Redis sorted set to use for storring mutexes. Defaults to "zmutexes".
            timeout_seconds (int, optional): Semaphore timeout. Defaults to 60.
            revalidate_seconds (int, optional): Semaphore zscore revalidation interval in locking wait(). Defaults to 10.
        """
        super(ZMutexSet, self).__init__()
        self.__redis = redis
        self.__name = '{}'.format(zsetname)
        self.__timeout = timeout_seconds * 1000000
        self.__revalidate = revalidate_seconds * 1000000.0
        
    def create(self, lock_id:str) -> bool:
        """Creates a semaphore in "expired" state

        Args:
            lock_id (str): Unique semaphore name

        Returns:
            bool: True when successfully created
        """
        res = self.__redis.zadd(name=self.__name, mapping={lock_id: 0}, nx=True)
        return res
    
    def __set_score(self, lock_id:str, old_zscore:Optional[float], now:float, new_zscore:Optional[float], locked:bool, pipeline:redis.client.Pipeline):
        """Transaction method (redis.transaction()) to update or delete zscore of elements in mutex sorted list.
        it assumes that redis.watch is set for the sorted list.
        Method is used internally by all public methods.
        Removes the element if new score is not provided.
        Before removing the element or updating the score validates if old_zscore didnt change.        

        Args:
            lock_id (str): redis sorted list's element key
            old_zscore (Optional[float]): old zscore of the element. Ignored if not provided
            now (float): the current timestemp (zscore representing current time)
            new_zscore (Optional[float]): New zscore to set (often equals to "now"). If None removes the element
            locked (bool): Indicates if target mutex should not be in "expired" state (when updating or releasing)
            pipeline (redis.client.Pipeline): Redis pipeline object

        Returns:
            [type]: [description]
        """
        zscore = pipeline.zscore(name=self.__name, value=lock_id)
        pipeline.multi()
        if zscore is None:
            return None
        if old_zscore is not None and old_zscore != zscore:
            return None
        if locked and zscore > now - self.__timeout:
            if new_zscore is None:
                pipeline.zrem(self.__name, lock_id)
            else:
                pipeline.zadd(name=self.__name, mapping={lock_id: new_zscore}, xx=True)
            return (lock_id, new_zscore)
        elif not locked and zscore < now - self.__timeout:
            if new_zscore is None:
                pipeline.zrem(self.__name, lock_id)
            else:
                pipeline.zadd(name=self.__name, mapping={lock_id: new_zscore}, xx=True)
            return (lock_id, new_zscore)
        return None
    
    def acquire(self, timeout=0, lock_id:Optional[str] = None) -> Optional[Tuple[str, float]]:
        """Acquires named semaphore, if name not provided acquires first "expired" semaphore. 
        Bocks call if timeout is greater than 0. 
        In blocking call revaluate "expire" every revalidate_seconds (only for this call) after start of the call
        Args:
            timeout (int, optional): Waits until acquire is possible or this number of seconds. 0 is not blocking. Defaults to 0.
            lock_id (Optional[str], optional): [description]. Defaults to None.

        Returns:
            Optional[Tuple[str, float]]: Semaphore structure. A tuple with semaphore key name and zscore of the key
            representing timestamp when the key was added/updated
        """
        
        #############################
        # this bit is taken from 
        # https://stackoverflow.com/questions/12317940/python-threading-can-i-sleep-on-two-threading-events-simultaneously/36661113
        def or_set(self):
            self._set()
            self.changed()

        def or_clear(self):
            self._clear()
            self.changed()

        def orify(e, changed_callback):
            e._set = e.set
            e._clear = e.clear
            e.changed = changed_callback
            e.set = lambda: or_set(e)
            e.clear = lambda: or_clear(e)

        def OrEvent(*events):
            or_event = threading.Event()
            def changed():
                bools = [e.is_set() for e in events]
                if any(bools):
                    or_event.set()
                else:
                    or_event.clear()
            for e in events:
                orify(e, changed)
            changed()
            return or_event
        #############################
        
        def _acquire(lock_id):
            locks = [lock_id] if lock_id is not None else self.get_stale()
            for lock_id in locks:
                now = datetime.now().timestamp() * 1000000
                f = lambda pipeline: self.__set_score(lock_id=lock_id, old_zscore=None, now=now, new_zscore=now, locked=False, pipeline=pipeline)
                res = self.__redis.transaction(f, self.__name, value_from_callable=True)
                if res:
                    return res
            return None
        
        def _wait(watches):
            for m in watches.listen():
                if m["type"] == "pmessage" and (m["data"] == b"zadd" or m["data"] == b"zincrby"):
                    return
        
        def _timer(timeout, event):
            # Sleep for min(timeout , self.__revalidate)
            _t = self.__revalidate / 1000000.0 if timeout > self.__revalidate / 1000000.0 else timeout
            sleep(_t)
            if event:
                event.set()
        
        def _waiter(watches, event):
            _wait(watches)
            if event:
                event.set()
        
        # Prepare 2 events. e1 ZSET pubsub listener and e2 Timeout Timer
        e1 = threading.Event()
        e2 = threading.Event()
        # event that fires when either of two are set
        or_e = OrEvent(e1, e2)
        
        start = datetime.now().timestamp()
        
        # Prepare ZSET watcher for pubsub listener
        p_watches = self.__redis.pubsub()
        namespace = '__keyspace@0__:{}'.format(self.__name)
        p_watches.psubscribe(namespace)
        
        # Run 2 threads to wait and fire each event separately.
        # Python cannot kill threads so need to make sure there is no runaway
        # 2nd thread always terminates (its a simple sleeper) so it doesnt have to be accounted for
        # 1st thread is a complex listener. Keep thread variable to call join() on it later to avoid runaway
        t1 = threading.Thread(target=_waiter, args=(p_watches, e1), name="Watch_{}".format(self.__name))
        threading.Thread(target=_timer, args=(timeout, e2), name="Sleep_{}_{}".format(timeout, start)).start()
        t1.start()
        
        # immediate attempt to acquire lock
        acquired = _acquire(lock_id)
        
        # if no locks acquired go into sleep and listen loop
        while acquired is None and timeout > 0:
            # wait timer or listener (whichever fires first)
            # If timer fired try to acquire, maybe there are new "expired"
            # If listener fired try to acquire, listener is armed for ZADD so there can be new lock
            or_e.wait()
            acquired = _acquire(lock_id)
            
            # if timeout expired braking
            if datetime.now().timestamp() > start + timeout:
                break
            
            # Reset the event (it could have been timer event)
            e2.clear()
            # Starting new timer thread (python cannot repurpose Threads)
            threading.Thread(target=_timer, args=(timeout, e2), name="Sleep_{}_{}".format(timeout, start)).start()
        
        # Unsubscribing the watcher. This also unblocks listener thread
        p_watches.punsubscribe()
        
        # Making sure listening thread not running away
        t1.join()
        return acquired
    
    def get_stale(self, include_unattended=True, include_expired=True) -> Collection[str]:
        if not (include_unattended or include_expired):
            return []
        start = 0 if include_unattended else 1
        end = 1 if not include_expired else int(datetime.now().timestamp() * 1000000) - self.__timeout
        return self.__redis.zrange(self.__name, start, end)
    
    def update(self, lock:Tuple[str, float]) -> Optional[Tuple[str, float]]:
        """Updates named semaphore.
        Fails when semaphore "expires" or semaphore was acquired by another actor.
        In blocking call revaluate "expire" every revalidate_seconds (only for this call) after start of the call

        Args:
            lock (Tuple[str, float]): semaphore structure

        Returns:
            Optional[Tuple[str, float]]: Updated semaphore structure
        """
        now = datetime.now().timestamp() * 1000000
        f = lambda pipeline: self.__set_score(lock_id=lock[0], old_zscore=lock[1], now=now, new_zscore=now, locked=True, pipeline=pipeline)
        res = self.__redis.transaction(f, self.__name, value_from_callable=True)        
        if res:
            return res
        return None
    
    def release(self, lock:Tuple[str, float], clean_up=True) -> Optional[Tuple[str, float]]:
        now = datetime.now().timestamp() * 1000000
        new_zscore = None if clean_up else 0
        f = lambda pipeline: self.__set_score(lock_id=lock[0], old_zscore=lock[1], now=now, new_zscore=new_zscore, locked=True, pipeline=pipeline)
        res = self.__redis.transaction(f, self.__name, value_from_callable=True)
        return res

    def delete(self, lock:Tuple[str, float]):
        now = datetime.now().timestamp() * 1000000
        f = lambda pipeline: self.__set_score(lock_id=lock[0], old_zscore=lock[1], now=now, new_zscore=None, locked=False, pipeline=pipeline)
        res = self.__redis.transaction(f, self.__name, value_from_callable=True)
        return res
