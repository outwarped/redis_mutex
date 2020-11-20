# Description

Distributed Mutex implementation with Redis sorted set [Redis Data Types](https://redis.io/topics/data-types)

## Key points

* Mutex is represented with a record in Redis sorted dataset

* Mutex name is used as record key. timestamp is used as record score

* When mutex is created its records score is set to 0, indicating that the mutex has never been acquired

* To acquire named mutex the corresponding record's score is checked to be _less than_ current timestamp minus mutex timeout delta (default 60s) and then record's score is updated with current timestamp

* To acquire any mutex first record in the score range from 0 to _less than_ current timestamp minus mutex timeout delta are checked. Then record's score is updated with current timestamp
  
* Blocking acquire call first checks if acquire is possible. If nothing is available it sleeps for at most revalidate_seconds (or until remaining time until timeout) or until Redis sorted list is updated (ZADD or ZINCRBY), whichever is fired first, then performs the same blocking flow again until something is acquired or no time remains until timeout.

* Valid acquired or updated mutex is returned as a tuple of mutex name and its new score (key, timestamp).

* To update or release the mutex the corresponding record's score is checked to be _more than_ (unlike acquire when it has to be _less than_) current timestamp minus mutex timeout delta and then record's score is updated with current timestamp.

* Record's score is checked to match its old score in mutex tuple (key, timestamp). This is done to avoid the edge case when "expired" mutex can still be updated after being required by another instance. If mutex was lost and then required by another instance its score would be different from the remembered old score.

* When mutex is release its key removed from the list of its score can be set to 0 again.

## Examples

Start redis:

```bash
docker run --name my-redis -p 6379:6379 -d redis
```

Python use case:

```python
from uuid import uuid4
from time import sleep
from threading import Thread
import redis
from redis_mutex import ZMutexSet as ZMutexSet

rpool = redis.ConnectionPool(host="localhost", port=6379, db=0)
r = redis.Redis(connection_pool=rpool)
mutexset = ZMutexSet(r, timeout_seconds=60)
mutex_name = str(uuid4())

def func1():
  # Let thread t3 enter blocking acquire()
  sleep(10)
  # Create named mutex mutex_name
  _ok = mutexset.create(mutex_name)

def func2():
  # Let thread t3 acquire mutex_name mutex
  sleep(20)
  # blocking (120s) call to acquire any available mutex
  # mutex_name becomes available timeout_seconds (60s) after last update/acquire
  _mutex = mutexset.acquire(timeout=120)
  sleep(10)
  _mutex = mutexset.release(mutex)

def func3():
  # blocking (30s) call to acquire named mutex mutex_name
  _mutex = mutexset.acquire(timeout=30, lock_id=mutex_name)
  sleep(10)  
  _mutex = mutexset.update(_mutex)
  # mutex mutex_name expires after timeout_seconds (60s) since last update/acquire

t1=Thread(target=func1)
t2=Thread(target=func2)
t3=Thread(target=func3)
t1.start()
t2.start()
t3.start()
t1.join()
t2.join()
t3.join()
```
