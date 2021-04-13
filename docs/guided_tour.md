# Guided Tour 🥾

This document provides a guided tour of Alsek. <br>
Please feel free to feed the functions. 🙂

## Backends

Alsek currently provides 'out of the box' support for two popular databases: [`Redis`](https://redis.io) 
and [`Sqite`](https://www.sqlite.org) (via [`DiskCache`](http://www.grantjenks.com/docs/diskcache/index.html)).

### Included

#### Redis

```python
from alsek.storage.backends.redis import RedisBackend

# Note: by default, `RedisBackend()` will attempt to 
# connect to an instance of Redis running on localhost.
backend = RedisBackend()
```

#### DiskCache

```python
from alsek.storage.backends.disk import DiskCacheBackend

backend = DiskCacheBackend()
```

!!! warning
    ``DiskCache`` persists data to a local (Sqlite) database and does not 
    implement 'server-side' "if not exist" on `SET` (`nx`) support. For these reasons, 
    ``DiskCacheBackend()`` is recommended for development and testing purposes only.

### Lazy Initialization

Both the `DiskCacheBackend` and `RedisBackend` support lazy initialization.
In this mode, an attempt to establish a connection with the database will be 
deferred until the first time it is absolutely needed (e.g., for a `SET`, `GET`, 
`DELETE`, etc.). This can be useful in applications such as REST APIs where the 
backend may not be available precisely when the application boots up. A small example 
of this mode is provided below.

```python
from redis import Redis
from alsek.storage.backends import LazyClient
from alsek.storage.backends.redis import RedisBackend

# Initialize the backend with LazyClient
lazy_backend = RedisBackend(LazyClient(lambda: Redis()))

# Run an operation. 
# This will cause LazyClient to evaluate the
# lambda and return a usable client connection.
lazy_backend.count("queues:math_ops")
```

### Custom Backends

Support for data backends that Alsek does not support natively can be achieved
by inheriting from the base `Backend` class. This requires overriding five core
methods: `exists()`, `set()`, `get()`, `delete()` and `scan()`.

```python
from typing import Any, Iterable, Optional, Union, Callable
from alsek.storage.backends import Backend, LazyClient


class CustomBackend(Backend):
    def __init__(self, conn: Union[Client, LazyClient], **kwargs) -> None:
        super().__init__(**kwargs)
        self._conn = conn

    @property
    def conn(self) -> Client:
        if isinstance(self._conn, LazyClient):  # lazy conn support
            self._conn = self._conn.get()
        return self._conn

    def exists(self, name: str) -> bool:
        self.conn
        ...

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        pass

    def get(self, name: str) -> Any:
        pass

    def delete(self, name: str, missing_ok: bool = False) -> None:
        pass

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        pass

```

The resulting `CustomBackend()` class can be used to power a broker
and, perhaps more importantly, a `ResultsStore`.


!!! note
    The `_conn`/`conn` pattern shown above can be skipped 
    if lazy client support is not needed.

### Namespaces

Backends store all of their data in a predefined 'namespace'.
This is simply a prefix that will be prepended to all "keys" (names) 
the backend writes to storage.

## Serializers

Prior to being written into the backend, Alsek requires that data is
first serialized. When data is read from the backend, it is deserialized
prior to use.

By default, Alsek uses JSON serialization, as implemented in 
`alsek.storage.serialization.JsonSerializer()`. However, another 
approach can be used, provided it is supported by the backend.
To use a different serialization procedure, one must:

  * create a new from serializer on top of the base `Serializer()` and 

  * pass the new serializer to the relevant backend at initialization time.

## Result Storage

Task results can be persisted to a `backend` using `ResultStore()`. 

```python
from typing import Dict

from alsek.core import Broker, task
from alsek.storage.backends.redis import RedisBackend
from alsek.storage.result import ResultStore

backend = RedisBackend("<connection_url>")

broker = Broker(backend)
result_store = ResultStore(backend)

@task(broker, result_store=result_store)
def valuable_output() -> Dict[str, int]:
    return {"a": 1, "b": 2, "c": 3}
```

We are also free to use different backends for `Broker` and `ResultStore`.
This is illustrated in the example below.

```python
from typing import Dict
from alsek.core import Broker, task
from alsek.storage.backends.redis import RedisBackend
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.result import ResultStore

redis_backend = RedisBackend()
disk_cache_backend = DiskCacheBackend()

broker = Broker(redis_backend)
result_store = ResultStore(disk_cache_backend)

@task(broker, result_store=result_store)
def valuable_output() -> Dict[str, int]:
    return {"a": 1, "b": 2, "c": 3}
```

!!! warning
    In order for data to be persisted via `result_store`, it must be
    of a type supported by the `backend`'s `serializer`.

## Messages

Messages are the "lingua franca" of Alsek. This is because the data they 
contain can be "understood" by all parts of the library from brokers and 
result stores, to tasks and worker pools.

### Properties

Messages have a lot of helpful functionality, the most
notable of which is explored here.

```python
from alsek.core import Broker, task
from alsek.storage.backends.disk import DiskCacheBackend

broker = Broker(DiskCacheBackend())

@task(broker)
def simple_task() -> int:
    return 99

message = simple_task.generate(args=(1, 1))
```

First, let's print the message:

```python
Message(
    task_name='simple_task',
    queue='alsek_queue',
    args=(1, 1),
    kwargs={},
    metadata=None,
    store_result=False,
    result_ttl=None,
    uuid='67de0e4c-9816-11eb-8cb0-acde48001122',
    progenitor=None,
    retries=0,
    timeout=3600000,
    created_at='2021-04-08 05:59:16.424000',
    updated_at='2021-04-08 05:59:16.424000',
    delay=0,
    previous_result=None,
    callback_message_data=None,
    backoff_settings={'algorithm': 'ExponentialBackoff', 'parameters': {'base': 4, 'factor': 10000, 'floor': 60000, 'ceiling': 3600000, 'zero_override': True}},
    mechanism='process',
    lock=None
)
```

Next, we can take a quick look at the key properties of the `message`
object.

```python
# Dictionary containing all of the data that can be,
# and will be, persisted to the backend by the broker.
message.data
# Output: {'task_name': 'simple_task', ...}

# High-level summary of the message
message.summary
# Output: Message(uuid='67de0e4c-9816-11eb-8cb0-acde48001122', queue='alsek_queue', task='simple_task')

# UTC Timestamp for when the message will next be ready for processing 
message.ready_at
# Output: 1617861556424

# Time until the message is ready for processing
message.ttr
# Output: 0

# Whether or not the message is ready for processing
message.ready
# Output: True
```

## Brokers

A `Broker` is responsible for adding and managing tasks on the backend.
While typical usage of Alsek will not usually involve directly interacting
with these capabilities, a high-level understanding of what they are may 
nevertheless prove helpful.

An Alsek broker provides the following core methods:

  * `sumbit()`: submit a message to a queue
  * `retry()`: retry a message on a queue
  * `remove()`: remove a message from the backend
  * `fail()`: remove a message, and move it to the Dead Letter Queue (DQL), if enabled

Additionally, `Broker` also exposes:

 * `get_subnamespace()`: computes the "subnamespace" in which a message exists.
    Practically, this is simply a prefix composed of `queue` name and `task` name
 * `get_message_name()`: computes the 'full' name of a message. Practically, this
    is simply its `subnamespace` plus its `uuid`.
 * `ack()`: acknowledge the message. (This is a convenience method and is 
    functionally the same as `remove()`.)
 * `nack()`: do not acknowledge the message and render it eligible
    for a redelivery.

## Tasks

### Mechanisms

Tasks can be executed on a _Worker Pool_ using either a `'thread'` or `'process'` mechanism.
We can specify which mechanism the worker pool should use when we construct the task.

```python
from alsek.core import task

@task(..., mechanism="thread")
def my_task() -> int:
    return 99
```

While the default mechanism is `'process'`, threads can be used in cases where 
lower overhead is desirable, or the task in question is largely I/O bound.  

!!! danger
    Many implementations of Python use a [Global Interpreter Lock](https://en.wikipedia.org/wiki/Global_interpreter_lock)
    (GIL), including the most common one: [CPython](https://en.wikipedia.org/wiki/CPython). In such implementations, 
    only one Python thread can do work at any one time. As a consequence, using `mechanism="thread"` 
    carries the risk of interfering with the threads used by the _Worker Pool_ itself (see below). In the worst 
    case, a task specified with `mechanism="thread"` may never relinquish the GIL and, as a result, the underlying 
    worker pool  will cease to function. For this reason, Alsek uses `mechansim='process'` by default.

    While this problem is relatively uncommon in practice, it is an important risk 
    to keep in mind when building your application.

### Timeouts

All Alsek tasks must include a `timeout` (in milliseconds), which is used to 
safeguard against hanging tasks. The default timeout is 3,600,000 milliseconds (1 hour).

Tasks which exceed their timeout will be shutdown by the worker
pool. If a task is eligible to be retried (see below) against a `TimeoutError`,
it will be. Otherwise, the corresponding message will be deleted.

```python
from alsek.core import task

@task(..., timeout=90 * 1000)  # lower timeout to 90 seconds
def my_task() -> int:
    return 99
```

!!! warning
    Enforcement of timeouts for tasks which use `mechansim='thread'` is only available
    for [CPython](https://en.wikipedia.org/wiki/CPython) (see below).

!!! warning
    Timeout enforcement for tasks which use `mechansim='thread'` is not as reliable as it
    is for tasks which use `mechansim='process'`. This is because Alsek effectuates timeouts in 
    thread tasks by asynchronously setting a `TimeoutError` inside them. In order for this error 
    to be raised, the thread in question must first acquire the GIL (see above). If the thread
    never acquires the GIL, the error will _never_ be raised. Conversely, Alsek implements timeouts
    for process tasks by directly terminating them, which is generally extremely reliable. The reasons
    for this dichotomy are beyond Alsek's control and stem from the implementation details of CPython
    itself.

    As above, while this problem is relatively uncommon in practice, it is an important risk 
    to keep in mind when building your application.

### Priority

As with timeouts, priority values can be set for each task.

Alsek implements _intra-queue_ task priority. In other words, task priority 
is enforced within, but not between, queues (which themselves can be prioritized).

Let's take a look at an example.

```python
from alsek.core import task

@task(..., queue="my_queue", priority=0)
def task_a() -> str:
    return "A!"

@task(..., queue="my_queue", priority=1)
def task_b() -> str:
    return "B!"
```

In Alsek, `priority` is inverted. That is, lower integers correspond to higher priority.
Thus, in the example above, instances of `task_a()` will _always_ take priority over 
instances of `task_b()`.

### Triggers

Alsek supports cron, date as well as interval triggers. Let's explore this using the example below.

```python
from alsek.core import Broker, task
from alsek.storage.backends.disk import DiskCacheBackend
from apscheduler.triggers.interval import IntervalTrigger

broker = Broker(DiskCacheBackend())

@task(broker, trigger=IntervalTrigger(hours=1))
def check_system_usage() -> int:
    return 99
```

The result will be a special of a type: `TriggerTask`.
Like `Task`, we can generate an instance of the task by
calling `generate()`.

```python
message = check_system_usage.generate()
```

The task will now be submitted to the broker every hour for as long 
as the Python process in which it was created is alive.

There are three main ways we can interrupt these kinds of tasks.

First, we can pause the scheduler:

```python
check_resource_usage.pause()

# This can be undone by running:
# check_resource_usage.resume()
```

Second, we can clear the scheduled task:

```python
check_resource_usage.clear()
```

Finally, we can shut down the underlying scheduler:

```python
check_resource_usage.shutdown()
```

!!! warning
    Function parameters are not permitted for tasks which use a trigger.

#### Result Storage

We can request storage of tasks with triggers, just as we did with a standard task above. 
However, fetching the results of a task requires us to know its `uuid`. While it is possible
to collect this information (e.g., via `pre_op()` or `post_op()`), it is often far easier to 
simply store the _progenitor_ message or, at the least, its `uuid`. With this information, 
we can obtain all _descendant_ messages.

```python
from random import randint
from apscheduler.triggers.interval import IntervalTrigger

from alsek.core import Broker, task
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.result import ResultStore

backend = DiskCacheBackend
broker = Broker(backend)
result_storage = ResultStore(backend)


@task(broker, result_store=result_storage, trigger=IntervalTrigger(seconds=10))
def harvest_data() -> int:
    data = randint(0, 100)
    return data

# Start 
message = harvest_data.generate()

# Get the result of the message. By setting `descendants=True` we
# will also data for any descendant messages which have completed.
results = result_storage.get(message, timeout=30 * 1000, descendants=True)
```

!!! warning
    The order of results when ``descendants=True`` is determined by the 
    time at which the data was written to the backend, not when the corresponding task 
    completed. While this difference is usually very small, if this is
    not appropriate for your application, you must include timestamp information 
    in the output of the task function and re-sort the results accordingly.

### Message Passing

The message itself will be passed to `task`s which include a `message` parameter. 

```python
from alsek.core import Broker, Message, task
from alsek.storage.backends.disk import DiskCacheBackend

broker = Broker(DiskCacheBackend())

@task(broker)
def my_task(message: Message) -> None:  # note: type hints are optional
    print(message.uuid)
```

!!! warning
    The message will not be passed to the task if:

       * a "message" key is included in `kwargs`, e.g., `my_task.generate(kwargs={"message": "string"})`
       * a type hint that does not resolve to `"Message"` is used for `message`.

### Callbacks

When a task completes, another task can be automatically triggered through
the use of a _callback_.

To see this, let's contrive two simple tasks: `add_1()` and `print_result()`:

```python
from alsek.core import Broker, Message, task
from alsek.storage.backends.disk import DiskCacheBackend

broker = Broker(DiskCacheBackend())

@task(broker)
def add_1(number: int) -> int:
    return number + 1

@task(broker)
def print_result(message: Message) -> None:
    print(message.previous_result)
```

In order to make `print_result()` execute when `add_1()`
completes, we simply need to pass a generated message to `callback`.
It's also advisable to set `submit=False` so that submission to the broker is 
deferred until _after_ the first message completes.

```python
add_1.generate(
    args=(1,),
    callback=print_result.generate(submit=False)
)
```

As a convenience, we can also use _deferred mode_, which instructs
the next call of `generate()` to skip submitting the message to
the broker.

```python
add_1.generate(
    args=(1,),
    callback=print_result.defer().generate()
)
```

#### Nested

Callbacks of arbitrary depth are also supported.

To see this, let's add another task into the mix.

```python
@task(broker)
def add_1_previous(message: Message) -> int:
    return message.previous_result + 1
    
```

```python
add_1.generate(
    args=(1,),
    callback=(
        add_1_previous.defer().generate(
            callback_message=add_1_previous.defer().generate(
                callback=add_1_previous.defer().generate(
                    callback=print_result.defer().generate()
                )
            ),
        )
    ),
)
```

While the code above will "work", it is very difficult to read. 
A better solution is to use a `tuple` of messages.

```python
add_1.generate(
    args=(1,),
    callback=(
        add_1_previous.defer().generate(),
        add_1_previous.defer().generate(),
        add_1_previous.defer().generate(),
        print_result.defer().generate()
    ),
)
```

Internally, a message nesting procedure will be run against the tuple passed to `callback`.
As a result, the two different approaches to multiple callbacks shown above are functionally 
_identical_.

!!! note
    The internal process described above for nesting a flat
    tuple of callbacks will update the `callback_message_data` 
    fields in the original messages.

!!! note
    Deferred mode is automatically cancelled by ``generate()``
    prior to it returning.

!!! warning
    Each callback message's `previous_result` and `progenitor` fields 
    will be set on the worker pool after successful execution of the 
    previous message, and are not available prior to this.

!!! warning
    The progenitor for a callback message is considered to be the previous message,
    *not* the root callback.  

!!! danger
    While it is valid for a task with a trigger to have callbacks,
    such tasks should not be used as callbacks themselves.

#### Control

If a callback is present for a message it will be executed by default.
However, it is possible to override this behaviour by reasoning about
the original message itself, the result of the task or both. 

```python
from typing import Any

from alsek.core import Message
from alsek.core.task import Task, task

class CustomTask1(Task):
    def do_callback(self, message: Message, result: Any) -> bool:
        if result > 1:
            return True
        else:
            return False

@task(..., base_task=CustomTask1)
def simple_task() -> int:
    return 99
```

!!! warning
    The `do_callback()` is only evaluated for messages which contain callbacks.

### Pre/Post Ops

The `pre_op()` and `post_op()` methods of `Task` can be used to 
perform operations before and/or after the `function` itself executes, respectively.
To do this, a new `base_task` must be created.

```python
from alsek.core import Message
from alsek.core.task import Task, task

class CustomTask2(Task):
    def pre_op(self, message: Message) -> None:
        print(f"About to process {message.summary}!")
        
    def post_op(self, message: Message, result: Any) -> None:
        print(f"Processed {message.summary} and got '{result}'!")


@task(..., base_task=CustomTask2)
def simple_task() -> int:
    return 99
```

### Retries

The number of times a task will be retried is determined by
`max_retries` by default.

In cases where this is not sufficiently sophisticated to determine if
message should be retried, the `do_retry()` method of the `Task`
class be overridden.

```python
from alsek.core import Message
from alsek.core.task import Task, task

class CustomTask3(Task):
    def do_retry(self, message: Message, exception: BaseException) -> bool:
        if isinstance(exception, ZeroDivisionError):
            return False
        elif self.max_retries is None:
            return True
        else:
            return message.retries < self.max_retries

@task(..., base_task=CustomTask3)
def simple_task() -> int:
    return 99
```

### Backoff

Rather than reprocessing a task immediately after it fails,
Alsek uses a backoff procedure. By default, `ExponentialBackoff()` 
is used with "sensible" default. However, the type of backoff algorithm 
as well as its parameters are extremely customizable. 

```python
from alsek.core import task
from alsek.core.backoff import (
    ConstantBackoff, 
    ExponentialBackoff,
    LinearBackoff, 
)

@task(..., backoff=ConstantBackoff(constant=30 * 1000))
def task_a() -> int:
    return 99

@task(..., backoff=LinearBackoff(factor=30 * 1000))
def task_b() -> int:
    return 99

@task(..., backoff=ExponentialBackoff(ceiling=90 * 1000))
def task_c() -> int:
    return 99
```

!!! note
    Backoff duration is determined by the number of 'incidents'.
    Here, an incident is a failed attempt to process the message.

!!! note
    Backoff can be disabled by using ``ConstantBackoff(constant=0, floor=0, ceiling=0, zero_override=True)``.

## Concurrency

Alsek's concurrency `Lock()` provides a straightforward way limit 
simultaneity across a distributed application, as shown here:

```python
from alsek.core import Lock, task
from alsek.storage.backends.disk import DiskCacheBackend

backend = DiskCacheBackend()

@task(...)
def send_data() -> int:
    with Lock("send_data", backend=backend, limit=1) as lock:
        if lock.acquire():
            """SEND DATA HERE"""
        else:
            print("Failed to acquire lock")
```

## Consumers

As their name suggests, consumers pull messages inserted by the broker
onto workers. A concurrency lock (similar to what is shown above) is
used to ensure than one, and only one, consumer can hold a message
at any given time.

Standard use of Alsek does not require direct interaction with consumers,
as they are managed by _Worker Pools_ (see below). However, in the interest 
of completeness, an illustrative example of how to do so is provided below.

```python
from alsek.core import Broker
from alsek.core.consumer import Consumer
from alsek.storage.backends.redis import RedisBackend

broker = Broker(RedisBackend())
consumer = Consumer(broker)

for message in consumer.stream():
    print(f"Got {message.summary}")
```

!!! note
    Consumers backoff following one or more passes over the backend that 
    do not yield any ready messages. By default, `LinearBackoff()` is used.

## CLI & Worker Pools

Worker pools create a collection of workers for tasks to be executed on.
Each pool relies on a `Consumer` to pull messages written to the backend
by the `Broker`.

When the worker pool reaches capacity, it will pause the stream of data
from the consumer. This is done to both reduce load on the backend and 
to allow other worker pools to acquire messages for processing.

### Basics

Typically, one will want to start worker pools using Alsek's command line interface (CLI). 
At a minimum, this requires us to specify where all task definitions can be found. 

#### Files

Suppose we have a file `my_task.py`. 
Then starting the pool requires us to run:

```shell
alsek my_tasks
```

#### Nested files

Alternatively, we might have a file of task definitions inside a directory, like this:

```
├── __init__.py
├── singletons.py
└── my_tasks.py
```

Starting a pool with this kind of structure can be accomplished by passing
the dot-separated path to the file: 

```shell
alsek my_project.my_tasks
```

#### Recursive

We can also simply specify the directory where the task definitions live, 
and it will be scanned recursively in order to recover _all_ task definitions.

```shell
alsek my_project
```

### Advanced options

Alsek's CLI includes many options to provide fine-grain control over the worker pool.
We won't cover all of them here, but there are at least three worth highlighting.

The first is the `-q`/`--queues` option. This allows one to limit the queues
which will be consumed by the worker pool. It can be set using a comma-separated list.

```shell
alsek my_project -q queue_a
```

```shell
alsek my_project -q queue_a,queue_b,queue_z
```

The `--max_threads` and `--max_processes` options are also noteworthy.
These options provide control over the maximum number of threads and 
processes that can run on the worker pool, respectively. The total number 
of workers in the pool is determined by the sum of these two numbers.

```shell
alsek my_project --max_threads 8
```

```shell
alsek my_project --max_processes 2
```

#### Multiprocessing Backend

Lastly, it is possible to configure the multiprocessing library used by a worker pool.
By default, [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html) from 
the Python standard library will be used. However, it is possible to switch to the [`PyTorch`](https://pytorch.org)
implementation of multiprocessing found [here](https://pytorch.org/docs/stable/multiprocessing.html).
This can be done by setting the `ALSEK_MULTIPROCESS_BACKEND` environment variable prior to booting
up the worker pool.

```shell
export ALSEK_MULTIPROCESS_BACKEND=pytorch

alsek my_project
```

!!! note
    The worker pool's `Consumer` will respect the order in which queues
    are listed for the `-q`/`--queues` option. If, this option is not specified, 
    queues will be consumed in alphabetical order.

!!! note
    Worker pools scale up and down dynamically based on load.

!!! note
    The full documentation for Alsek's CLI can be obtained by
    running:

    ```shell
    alsek --help
    ```
