# Tips & Tricks ⛷️

## Multiprocessing Backend

It is possible to configure the multiprocessing library used by a worker pool.
By default, [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html) from 
the Python standard library will be used. However, this can be changed to the [`PyTorch`](https://pytorch.org)
[implementation](https://pytorch.org/docs/stable/multiprocessing.html) of multiprocessing
by setting `ALSEK_MULTIPROCESSING_BACKEND` to `'torch'` prior to starting the worker pool.

```shell
export ALSEK_MULTIPROCESSING_BACKEND=torch

alsek my_project
```

## Testing

Testing an application may require a worker pool to be brought online.
A small example of how to do this with [pytest](https://docs.pytest.org/en/stable) 
and multiprocessing is provided below.

First, create a `conftest.py` file with a `background_worker_pool` figure. 

```python
import pytest
from multiprocessing import Process
from typing import List

from alsek import task
from alsek.core.worker import WorkerPool

from my_application.tasks import task_1, task_2


def _run_pool() -> None:
    WorkerPool([task_1, task_2], backoff=None).run()


@pytest.fixture()
def background_worker_pool() -> None:
    process = Process(target=_run_pool, daemon=True)
    process.start()
    yield 
    process.terminate()
```

Now, we can bring a worker pool online simply by including
`background_worker_pool` in the signature of a test.

```python
from alsek import task
from my_application.tasks import task_1


def test_processing(background_worker_pool: None) -> None:
    message = task_1.generate()
    ...
```
