# Alsek ⛰

## Overview

Alsek is a distributed task queue library for Python. It has been built to be highly 
capable while remaining very lightweight. Currently, the following functionality
is available:

  * Multiprocess and multi-thread task execution
  * Automatic retries, with customizable backoff procedures
  * Prioritization of queues and tasks
  * Task Status
  * Result storage
  * Cron, date and interval task triggers with result storage support
  * Robust task timeouts
  * Callbacks
  * Dead Letter Queue (DLQ)

## Installing

```shell
pip install git+git://github.com/TariqAHassan/alsek@master
```

Note: Alsek will be added to PyPI in the near future.

Requires Python 3.7+

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

Task generation yields a `Message() `object, which contains detailed information about the task. 
We can see this information by simply printing the message:

```python
Message(
    task_name='add',
    queue='math_ops',
    args=(1, 1),
    kwargs={},
    metadata=None,
    store_result=False,
    result_ttl=None,
    uuid='e49806be-96ad-11eb-9216-acde48001122',
    progenitor=None,
    retries=0,
    timeout=3600000,
    created_at='2021-04-06 10:00:00.360000',
    updated_at='2021-04-06 10:00:00.360000',
    delay=0,
    previous_result=None,
    callback_message_data=None,
    backoff_settings={'algorithm': 'ExponentialBackoff', 'parameters': {'base': 4, 'factor': 10000, 'floor': 60000, 'ceiling': 3600000, 'zero_override': True}},
    mechanism='process',
    lock=None
)
```

Here we can see the name of the original task was "add" and that it was enqueued on the "math_ops" queue. 
We can also see more advanced information, such as the backoff procedure that will be used in the case of 
retries as well as the mechanism that will use to execute the task. 
(In this case, the _Worker Pool_ will execute the task in a distinct process.)

In order for this message to be processed, we will need to start up a worker pool. 
This can be accomplished using Alsek's command line interface (CLI).

When spinning up a worker pool, we need to specify the modules where the tasks are defined. 
In our case, this means passing `my_tasks`. (Note that Alsek scans this argument recursively, 
allowing for task definitions to be nested deep inside directories.)

```shell
alsek my_tasks --verbose
```

Because the `--verbose` flag was included in the command above, detailed 
information about the workers will be logged to the console, which we can see below.

```shell
[2021-04-06 10:00:30.250] [PID 74896] [INFO] alsek.core.worker: Alsek v0.0.1 worker pool booting up...
[2021-04-06 10:00:30.251] [PID 74896] [INFO] alsek.core.worker: Worker pool online.
[2021-04-06 10:00:30.260] [PID 74897] [INFO] alsek.core.worker: Received Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add')...
[2021-04-06 10:00:30.261] [PID 74897] [INFO] alsek.core.worker: Successfully processed Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add').
```

Above we can see that once the worker pool boot up, a worker consumed the 
message we created above and then processed it successfully.

---

## Alsek?

This library is named after the [Tatshenshini-Alsek](https://en.wikipedia.org/wiki/Tatshenshini-Alsek_Provincial_Park)
Park and UNESCO World Heritage Site in northern British Columbia, Canada.
