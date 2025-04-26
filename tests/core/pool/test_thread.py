"""

    Test Thread Pool

"""

from __future__ import annotations

import queue as _q
import time
from multiprocessing import Event, Queue as MPQueue
from queue import Queue
from typing import Any

from alsek import Message
from alsek.core.pools.thread import (
    ThreadWorkerPool,
    ProcessGroup,
    ThreadInProcessGroup,
)


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #


def _queue_has_items(q: Queue) -> bool:
    """Cross-platform check for non-empty stdlib Queue / multiprocessing.Queue."""
    try:  # Linux / Windows
        return q.qsize() > 0
    except (NotImplementedError, AttributeError):  # fallback
        return not q.empty()


# ------------------------------------------------------------------ #
# 1. tasks are consumed end-to-end
# ------------------------------------------------------------------ #


def test_tasks_consumed(rolling_thread_worker_pool: ThreadWorkerPool) -> None:
    pool = rolling_thread_worker_pool
    # emit one message per task
    for t in pool.tasks:
        t.generate()

    pool.run()  # fixture makes this non-blocking
    assert any(_queue_has_items(g.queue) for g in pool._progress_groups)


# ------------------------------------------------------------------ #
# 2–3. submit_message()
# ------------------------------------------------------------------ #


def test_submit_message_available_slot(
    rolling_thread_worker_pool: ThreadWorkerPool,
) -> None:
    pool = rolling_thread_worker_pool
    msg = pool.tasks[0].generate()
    assert pool.submit_message(msg) is True


def test_submit_message_pool_saturated(
    monkeypatch,
    rolling_thread_worker_pool: ThreadWorkerPool,
) -> None:
    pool = rolling_thread_worker_pool
    # pretend there are no free groups
    monkeypatch.setattr(pool, "_acquire_group", lambda: None)
    msg = pool.tasks[0].generate()
    assert pool.submit_message(msg) is False


# ------------------------------------------------------------------ #
# 4. ProcessGroup.has_slot()
# ------------------------------------------------------------------ #


def test_process_group_has_slot() -> None:
    g = ProcessGroup(
        n_threads=2,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )
    assert g.has_slot() is True
    # fill the queue
    g.queue.put(b"x")
    g.queue.put(b"y")
    assert g.has_slot() is False
    g.stop()


# ------------------------------------------------------------------ #
# 5. ProcessGroup.submit() – success & queue.Full path
# ------------------------------------------------------------------ #
class _DummyTask:
    @staticmethod
    def serialize() -> dict[str, Any]:
        return {}


class _DummyMsg(Message):  # minimal stub; only .data accessed
    data: dict[str, Any] = {}


def test_process_group_submit_success(monkeypatch) -> None:
    g = ProcessGroup(
        n_threads=1,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )
    # ensure put() doesn’t raise so we hit the happy path
    monkeypatch.setattr(g.queue, "put", lambda *a, **kw: None)
    assert (
        g.submit(
            _DummyTask(),  # noqa
            _DummyMsg("dummy_task_name"),  # noqa
        )
        is True
    )
    g.stop()


def test_process_group_submit_when_full(monkeypatch) -> None:
    g = ProcessGroup(
        n_threads=1,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )

    def _raise_full(*_a, **_kw):
        raise _q.Full

    monkeypatch.setattr(g.queue, "put", _raise_full)
    assert (
        g.submit(
            _DummyTask(),  # noqa
            _DummyMsg("dummy_task_name"),  # noqa
        )
        is False
    )
    g.stop()


# ------------------------------------------------------------------ #
# 6. ProcessGroup.stop() terminates child process
# ------------------------------------------------------------------ #
def test_process_group_stop_terminates_process() -> None:
    g = ProcessGroup(
        n_threads=1,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )
    assert g.process.is_alive()
    g.stop(timeout=0.2)
    assert not g.process.is_alive()


# ------------------------------------------------------------------ #
# 7. ThreadInProcessGroup._has_capacity()
# ------------------------------------------------------------------ #
def test_thread_in_process_group_has_capacity() -> None:
    tig = ThreadInProcessGroup(
        q=MPQueue(),
        shutdown_event=Event(),
        n_threads=2,
        slot_wait_interval=0.01,
    )
    assert tig._has_capacity() is True
    # simulate running futures
    tig._live.extend([object()])  # noqa
    assert tig._has_capacity() is True
    tig._live.extend([object()])  # noqa
    assert tig._has_capacity() is False


# ------------------------------------------------------------------ #
# 8. ThreadWorkerPool.prune() removes exited groups
# ------------------------------------------------------------------ #
def test_thread_worker_pool_prune_removes_dead_groups(
    rolling_thread_worker_pool: ThreadWorkerPool,
) -> None:
    pool = rolling_thread_worker_pool
    # add an extra group and kill it
    dead_group = ProcessGroup(
        n_threads=1,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )
    pool._progress_groups.append(dead_group)
    dead_group.stop(timeout=0.05)
    time.sleep(0.05)  # allow OS to reap

    pool.prune()
    assert dead_group not in pool._progress_groups
