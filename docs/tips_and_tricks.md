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

## Capturing Status Updates

In some applications it may be desirable to persists status updates 
to multiple locations. For example, a Redis database and a PostgreSQL database.
A short example of how to do this is provided below.

```python
from alsek.core.message import Message
from alsek.core.status import StatusTracker, TaskStatus

from sqlalchemy import Column, String, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

engine = create_engine("...")
Session = sessionmaker(bind=engine)
session = Session()


class Status(Base):
    __tablename__ = "status"
    
    id = Column(UUID, primary_key=True)
    status = Column(String)

    def __repr__(self) -> str:
        return f"<Status(id='{self.id}', status='{self.status}')>"

    
class CustomStatusTracker(StatusTracker):
    def set(self, message: Message, status: TaskStatus) -> None:
        super().set(message, status=status)
        
        if status == TaskStatus.SUBMITTED:
            session.add(Status(id=message.uuid, status=status.name))
        else:
            record = session.query(Status).get(message.uuid)
            record.status = status.name
        session.commit()
```

This new `CustomStatusTracker()` class is a drop-in replacement for `StatusTracker()`.

## Testing

Testing an application may require a worker pool to be brought online.
A small example of how to do this with [pytest](https://docs.pytest.org/en/stable) 
and multiprocessing is provided below.

First, create a `conftest.py` file with a `background_worker_pool` figure. 

```python
import pytest
from multiprocessing import Process

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

Now, a worker pool can be brought online simply by including
`background_worker_pool` in the signature of a test.

```python
from alsek import task
from my_application.tasks import task_1


def test_processing(background_worker_pool: None) -> None:
    message = task_1.generate()
    ...
```
