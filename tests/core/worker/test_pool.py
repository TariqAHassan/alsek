"""

    Test Worker Pool

"""
import copy
from typing import Type

import pytest

from alsek.core.futures import ProcessTaskFuture, ThreadTaskFuture
from alsek.core.message import Message
from alsek.core.task import task
from alsek.core.worker import WorkerPool
from tests._helpers import sleeper


def _futures_count(worker_pool: WorkerPool) -> bool:
    return sum(len(i) for i in worker_pool._futures.values())


def _constant_loop() -> None:
    while True:
        sleeper(10)


class MockFuture:
    def __init__(self, complete: bool, time_limit_exceeded: bool) -> None:
        self.complete = complete
        self.time_limit_exceeded = time_limit_exceeded

    def stop(self, exception: Type[BaseException]) -> None:
        pass


def test_tasks_consumed(rolling_worker_pool: WorkerPool) -> None:
    # Validate that there are no futures to start with
    assert not _futures_count(rolling_worker_pool)

    # Generate an instance of each task
    messages = list()
    for t in rolling_worker_pool.tasks:
        msg = t.generate()
        assert t.broker.exists(msg)
        messages.append(msg)

    # Poll the backend
    rolling_worker_pool.run()
    assert _futures_count(rolling_worker_pool)


def test_stop_all_futures(rolling_worker_pool: WorkerPool) -> None:
    # Create constant tasks
    thread_task = task(rolling_worker_pool.broker, mechanism="thread")(_constant_loop)
    process_task = task(rolling_worker_pool.broker, mechanism="process")(_constant_loop)

    # Override the tasks
    rolling_worker_pool.tasks = [thread_task, process_task]
    rolling_worker_pool._task_map = {t.name: t for t in rolling_worker_pool.tasks}

    # Submit instances of each task
    rolling_worker_pool._add_future(thread_task.generate())
    rolling_worker_pool._add_future(process_task.generate())

    # Check all the futures are active
    assert all(not f.complete for v in rolling_worker_pool._futures.values() for f in v)

    # Stop the futures
    rolling_worker_pool._stop_all_futures()
    # Wait
    sleeper(100)
    # Check all of the futures are 'complete'
    for v in rolling_worker_pool._futures.values():
        for future in v:
            assert future.complete


@pytest.mark.parametrize("mode", ["complete", "time_limit_exceeded"])
def test_manage_futures(
    mode: str,
    rolling_worker_pool: WorkerPool,
) -> None:
    # Create mock futures
    thread_future = MockFuture(
        complete=mode == "complete",
        time_limit_exceeded=mode == "time_limit_exceeded",
    )
    process_future = copy.deepcopy(thread_future)

    # Add the futures manually
    rolling_worker_pool._futures["thread"].append(thread_future)
    rolling_worker_pool._futures["process"].append(process_future)

    # Manage the futures
    rolling_worker_pool._manage_futures()

    # Check that all of the futures have been removed
    assert not _futures_count(rolling_worker_pool)


@pytest.mark.parametrize("fill", [True, False])
def test_slot_available(
    fill: bool,
    rolling_worker_pool: WorkerPool,
) -> None:
    max_threads = rolling_worker_pool.max_threads
    max_processes = rolling_worker_pool.max_processes

    if fill:
        rolling_worker_pool._futures["thread"] += list(range(max_threads))
        rolling_worker_pool._futures["process"] += list(range(max_processes))

    assert rolling_worker_pool._slot_available("thread") is not fill
    assert rolling_worker_pool._slot_available("process") is not fill


@pytest.mark.parametrize(
    "message,slot_available",
    [
        (Message("task", mechanism="process"), True),
        (Message("task", mechanism="thread"), True),
        (Message("task", mechanism="process"), False),
        (Message("task", mechanism="thread"), False),
    ],
)
def test_ready(
    message: Message,
    slot_available: bool,
    rolling_worker_pool: WorkerPool,
) -> None:
    rolling_worker_pool._slot_available = lambda _: slot_available
    assert rolling_worker_pool._ready(message, wait=False) is slot_available


def test_make_future(rolling_worker_pool: WorkerPool) -> None:
    # Create override tasks
    thread_task = task(rolling_worker_pool.broker, mechanism="thread")(lambda: 1)
    process_task = task(rolling_worker_pool.broker, mechanism="process")(lambda: 1)

    # Override the tasks
    rolling_worker_pool.tasks = [thread_task, process_task]
    rolling_worker_pool._task_map = {t.name: t for t in rolling_worker_pool.tasks}

    # Check that the correct type of task is generated
    assert isinstance(
        rolling_worker_pool._make_future(thread_task.generate()), ThreadTaskFuture
    )
    assert isinstance(
        rolling_worker_pool._make_future(process_task.generate()), ProcessTaskFuture
    )


def test_add_future(rolling_worker_pool: WorkerPool) -> None:
    for t in rolling_worker_pool.tasks:
        rolling_worker_pool._add_future(t.generate())

    assert _futures_count(rolling_worker_pool) == len(rolling_worker_pool.tasks)


def test_add_future_unknown_task(rolling_worker_pool: WorkerPool) -> None:
    unknown_task = task(rolling_worker_pool.broker, name="unknown")(lambda: 1)

    rolling_worker_pool._add_future(unknown_task.generate())
    assert _futures_count(rolling_worker_pool) == 0


def test_run(rolling_worker_pool: WorkerPool) -> None:
    assert not rolling_worker_pool._pool_manager.running
    rolling_worker_pool.run()
    assert not rolling_worker_pool._pool_manager.running
