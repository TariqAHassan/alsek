<div align="center">
  <img src="https://github.com/TariqAHassan/alsek/raw/master/docs/assets/logos/logo.png"><br>
</div>

----

Alsek is a distributed task queue library for Python. It has been built to be highly 
capable while remaining very lightweight. Currently, the following functionality
is available:

  * Multi-process and multi-thread task execution mechanisms
  * Support for multiple backends, with an easy-to-use extension API
  * Automatic retries, with customizable backoff procedures
  * Prioritization of queues and tasks
  * Result storage
  * Cron, date and interval task triggers with result storage support
  * Robust task timeouts
  * Callbacks
  * Dead Letter Queue (DLQ)

## Installing

Alsek can be installed with `pip` as follows:

```shell
pip install alsek
```

The optional `diskcache` dependency can be included by running:

```shell
pip install 'alsek[diskcache]'
```

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

Now, we can start up a pool of workers on the command line:

```shell
$ alsek quick_start --verbose
```

Which will result in output similar to this:

```shell
[2021-04-06 10:00:30.250] [PID 74896] [INFO] alsek.core.worker: Alsek v0.0.1 worker pool booting up...
[2021-04-06 10:00:30.251] [PID 74896] [INFO] alsek.core.worker: Worker pool online.
[2021-04-06 10:00:30.260] [PID 74897] [INFO] alsek.core.worker: Received Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add')...
[2021-04-06 10:00:30.261] [PID 74897] [INFO] alsek.core.worker: Successfully processed Message(uuid='e49806be-96ad-11eb-9216-acde48001122', queue='math_ops', task='add').
```

A longer introduction to Alsek can be found in [Getting Started]().

## Documentation

Documentation can be found [here]().

## Examples

Examples can be found in the [examples](examples) directory.
