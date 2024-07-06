<div align="center">
  <img src="https://github.com/TariqAHassan/alsek/raw/master/docs/assets/logos/logo.png"><br>
</div>

<p align="center">
    <a href="https://github.com/TariqAHassan/alsek/actions/workflows/tests.yml" alt="Tests">
        <img src="https://github.com/TariqAHassan/alsek/actions/workflows/tests.yml/badge.svg" /></a>
</p>

----

Alsek is a distributed task queue library for Python. It has been built to be highly 
capable while remaining very lightweight. Current functionality includes:

  * Multiprocess and multi-thread task execution
  * Automatic retries, with customizable backoff procedures
  * Prioritization of queues and tasks
  * Result storage
  * Status Tracking and Publishing
  * Interactive Result Iteration
  * Cron, date and interval task triggers (with result storage support)
  * Robust task timeouts
  * Callbacks
  * Dead Letter Queues (DLQs)

## Installing

```shell
pip install git+git://github.com/TariqAHassan/alsek@master
```

Note: Alsek will be added to PyPI in the near future.

Requires Python 3.7+

## Quick Start

First, add the following to a file named `quick_start.py`:

```python
from alsek import Broker, task
from alsek.storage.backends.redis import RedisBackend

backend = RedisBackend()  # uses localhost by default
broker = Broker(backend)


@task(broker, queue="math_ops")
def add(a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    message = add.generate(args=(1, 1))
    print(f"Task submitted. UUID: {message.uuid}.")
```

Running the script will generate an instance of the task.

```shell
python quick_start.py
# Task submitted. UUID: e49806be-96ad-11eb-9216-acde48001122.
```

Now, we can start up a pool of workers on the command line:

```shell
$ alsek quick_start --verbose
```

Which will result in output similar to this:

```shell
[2021-04-06 10:00:30.250] [MainProcess] [MainThread] [INFO] alsek.core.worker: Alsek v0.0.1 worker pool booting up...
[2021-04-06 10:00:30.251] [MainProcess] [MainThread] [INFO] alsek.core.worker: Worker pool online.
[2021-04-06 10:00:30.260] [Process-1] [MainThread] [INFO] alsek.core.worker: Received Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add')...
[2021-04-06 10:00:30.261] [Process-1] [MainThread] [INFO] alsek.core.worker: Successfully processed Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add').
```

As we can see above, the message was quickly processed by the worker pool.

If you would like a longer introduction to Alsek, see [Getting Started](https://tariqahassan.github.io/alsek/#getting-started).

## Documentation

Documentation can be found [here](https://TariqAHassan.github.io/alsek/).

## Examples

Examples can be found in the [examples](examples) directory.
