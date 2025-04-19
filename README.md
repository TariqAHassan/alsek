<div align="center">
  <img src="https://github.com/TariqAHassan/alsek/raw/main/docs/assets/logos/logo.png"><br>
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
pip install git+https://github.com/TariqAHassan/alsek@main
```

Note: Alsek will be added to PyPI in the near future.

Requires Python 3.9+

### Development

For development, it might also be helpful to install `redis`.

```shell
# MacOS
brew install redis
brew services start redis
```

## Quick Start

First, add the following to a file named [`tasks.py`](examples/simple/tasks.py):

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
python tasks.py
# Task submitted. UUID: e49806be-96ad-11eb-9216-acde48001122.
```

Now, we can start up a pool of workers on the command line:

```shell
$ alsek tasks
```

Which will result in output similar to this:

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

As we can see above, the message was quickly processed by the worker pool.

If you would like a longer introduction to Alsek, see [Getting Started](https://tariqahassan.github.io/alsek/#getting-started).

## Documentation

Documentation can be found [here](https://TariqAHassan.github.io/alsek/).

## Examples

Examples can be found in the [examples](examples) directory.
