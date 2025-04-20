# Alsek ⛰

## Overview

Alsek is a distributed task queue library for Python. It has been built to be highly 
capable while remaining very lightweight. Current functionality includes:

  * Multiprocess and multi-thread task execution
  * Automatic retries, with customizable backoff procedures
  * Prioritization of queues and messages
  * Result storage
  * Status Tracking and Publishing
  * Interactive Result Iteration
  * Cron, date and interval task triggers (with result storage support)
  * Robust task timeouts
  * Revoking tasks
  * Callbacks
  * Dead Letter Queues (DLQs)

## Installing

```shell
pip install git+git://github.com/TariqAHassan/alsek@master
```

Note: Alsek will be added to PyPI in the near future.

Requires Python 3.9+

## Getting Started

Alsek provides a framework for distributing, managing and executing tasks. 
It accomplishes this using two core pieces of machinery: brokers and workers. 
Brokers are responsible for the distribution and management of tasks, while 
workers are  responsible for task execution. Let's work though each of these
pieces in order.

First, we'll create a `Broker()` powered by a `Redis` backend. 
Let's suppose we do this in a file named `singletons.py`.

```python
from alsek import Broker
from alsek.storage.backends.redis import RedisBackend

backend = RedisBackend("<connection_url>")
broker = Broker(backend)
```

Next, we can define a task to perform. Let's imagine we do this in a file named `my_tasks.py`.

```python
from alsek import task
from singletons import broker

@task(broker, queue="math_ops")
def add(a: int, b: int) -> int:
    return a + b
```
    
When we generate an instance of this task, as shown below, it will be persisted to the backend by the broker.

```python
message = add.generate(args=(1, 1))
```

Task generation yields a `Message()` object, which contains detailed information about the task. 
We can see this information by simply printing the message:

```python
Message(
    task_name='add',
    queue='math_ops',
    args=(1, 1),
    kwargs={},
    metadata=None,
    result_ttl=None,
    uuid='e49806be-96ad-11eb-9216-acde48001122',
    progenitor_uuid=None,
    retries=0,
    timeout=3600000,
    created_at='2021-04-06 10:00:00.360000',
    updated_at='2021-04-06 10:00:00.360000',
    delay=0,
    previous_result=None,
    previous_message_uuid=None,
    callback_message_data=None,
    backoff_settings={'algorithm': 'ExponentialBackoff', 'parameters': {'base': 4, 'factor': 10000, 'floor': 60000, 'ceiling': 3600000, 'zero_override': True}},
    mechanism='process',
)
```

Here we can see the name of the original task was "add" and that it was enqueued on the "math_ops" queue. 
We can also see more advanced information, such as the backoff procedure that will be used in the case of 
retries as well as the mechanism that will be use to execute the task. 
(In this case, the _Worker Pool_ will execute the task in a distinct process.)

In order for this message to be processed, we will need to start up a worker pool. 
This can be accomplished using Alsek's command line interface (CLI).

When spinning up a worker pool, we need to specify the modules where the tasks are defined. 
In our case, this means passing `my_tasks`. (Note that Alsek scans this argument recursively, 
allowing for task definitions to be nested deep inside directories.)

```shell
alsek my_tasks
```

Now, detailed information about the workers will be logged to the console, which we can see below.

```shell
[2025-04-19 18:30:39.522] [MainProcess] [MainThread] [INFO] alsek.core.worker: Alsek v0.8.0 worker pool booting up...
[2025-04-19 18:30:39.522] [MainProcess] [MainThread] [INFO] alsek.core.worker: Starting worker pool with 8 max thread(s) and 11 max process(es)...
[2025-04-19 18:30:39.523] [MainProcess] [MainThread] [INFO] alsek.core.worker: Monitoring 1 queue(s).
[2025-04-19 18:30:39.523] [MainProcess] [MainThread] [INFO] alsek.core.worker: Worker pool online.
[2025-04-19 18:30:39.524] [MainProcess] [Thread-2 (_wrapper)] [INFO] alsek.core.futures: Received Message(uuid='bf90af06-1d6d-11f0-affc-4af50920870b', queue='math_ops', task='add')...
[2025-04-19 18:30:39.524] [MainProcess] [Thread-2 (_wrapper)] [INFO] alsek.core.futures: Successfully processed Message(uuid='bf90af06-1d6d-11f0-affc-4af50920870b', queue='math_ops', task='add').
[2025-04-19 18:30:39.524] [MainProcess] [Thread-2 (_wrapper)] [INFO] alsek.core.broker: Removing Message(uuid='bf90af06-1d6d-11f0-affc-4af50920870b', queue='math_ops', task='add')...
[2025-04-19 18:30:39.525] [MainProcess] [Thread-2 (_wrapper)] [INFO] alsek.core.broker: Removed Message(uuid='bf90af06-1d6d-11f0-affc-4af50920870b', queue='math_ops', task='add').
```

Above we can see that once the worker pool booted up, a worker consumed the 
message we created above and then processed it successfully.

---

## Alsek?

This library is named after the [Tatshenshini-Alsek](https://en.wikipedia.org/wiki/Tatshenshini-Alsek_Provincial_Park)
Park and UNESCO World Heritage Site in northern British Columbia, Canada.
