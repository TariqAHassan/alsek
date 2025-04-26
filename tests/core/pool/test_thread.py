"""

    Test Thread Pool

"""

from __future__ import annotations

import queue as _q
from multiprocessing import Event, Queue as MPQueue
from queue import Queue
from typing import Any

import pytest

from alsek import Message, Broker, StatusTracker
from alsek.core.pools.thread import (
    ThreadWorkerPool,
    ProcessGroup,
    ThreadInProcessGroup,
)
from alsek.core.status import TaskStatus, TERMINAL_TASK_STATUSES
from alsek.core.task import task
import multiprocessing as mp
import threading
from pathlib import Path


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
    monkeypatch: pytest.MonkeyPatch,
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


def test_process_group_submit_success(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_process_group_submit_when_full(monkeypatch: pytest.MonkeyPatch) -> None:
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


@pytest.mark.flaky(max_runs=2)
def test_process_group_stop_terminates_process() -> None:
    g = ProcessGroup(
        n_threads=1,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )
    assert g.process.is_alive()
    g.stop()
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


# ------------------------------------------------------------------ #
# 9. ThreadInProcessGroup._prune() drops finished & timed-out futures
# ------------------------------------------------------------------ #


def test_thread_in_process_group_prune_behavior() -> None:
    """• remove objects whose .complete is True
    • kill + drop objects whose .time_limit_exceeded is True
    • keep live, incomplete futures"""
    tig = ThreadInProcessGroup(
        q=MPQueue(),
        shutdown_event=Event(),
        n_threads=10,
        slot_wait_interval=0.01,
    )

    class _FakeFuture:
        def __init__(self, *, complete: bool, tlex: bool):
            self.complete = complete
            self.time_limit_exceeded = tlex
            self.stop_called = False
            self.cleaned = False

        # signature must match real ThreadTaskFuture.stop / clean_up
        def stop(self, _exc) -> None:  # noqa: D401
            self.stop_called = True

        def clean_up(self, ignore_errors: bool = False) -> None:
            self.cleaned = True

    live = _FakeFuture(complete=False, tlex=False)
    finished = _FakeFuture(complete=True, tlex=False)
    timed_out = _FakeFuture(complete=False, tlex=True)

    tig._live = [live, finished, timed_out]
    tig._prune()

    # finished & timed-out should be gone; live remains
    assert tig._live == [live]
    # timed_out future must have been stopped & cleaned
    assert timed_out.stop_called is True and timed_out.cleaned is True


# ------------------------------------------------------------------ #
# 10-12. ThreadWorkerPool._acquire_group() logic
# ------------------------------------------------------------------ #


def _mk_pool(broker: Broker) -> ThreadWorkerPool:
    """Spin up a tiny pool with a single dummy thread-task."""
    from alsek.core.task import task

    dummy_task = task(broker, name="dummy", mechanism="thread")(lambda: None)
    return ThreadWorkerPool(
        tasks=[dummy_task],
        n_threads=1,
        n_processes=2,  # small cap so we can hit it quickly
    )


def test_acquire_group_reuses_available_slot(rolling_broker: Broker) -> None:
    pool = _mk_pool(rolling_broker)
    g1 = pool._acquire_group()  # spawns first group
    assert g1 is not None
    # queue still empty → should reuse
    assert pool._acquire_group() is g1
    g1.stop()
    pool.on_shutdown()


def test_acquire_group_spawns_until_cap(rolling_broker: Broker) -> None:
    pool = _mk_pool(rolling_broker)

    g1 = pool._acquire_group()
    assert len(pool._progress_groups) == 1

    # saturate first group
    g1.queue.put(b"x")

    g2 = pool._acquire_group()  # new spawn
    assert g2 is not g1
    assert len(pool._progress_groups) == 2

    # saturate second group as well
    g2.queue.put(b"y")

    # cap reached (n_processes=2) so now returns None
    assert pool._acquire_group() is None
    g1.stop()
    g2.stop()
    pool.on_shutdown()


def test_submit_message_fails_when_every_group_full(rolling_broker: Broker) -> None:
    pool = _mk_pool(rolling_broker)
    # create groups and fill each queue
    g1 = pool._acquire_group()
    g1.queue.put(b"x")
    g2 = pool._acquire_group()
    g2.queue.put(b"y")

    # generate a real Message object to pass in
    msg = pool.tasks[0].generate()
    assert pool.submit_message(msg) is False

    g1.stop()
    g2.stop()
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 9. Normal FLow
# ------------------------------------------------------------------ #


import time

@pytest.mark.timeout(10)
@pytest.mark.flaky(max_runs=2)
def test_task_completes_successfully(rolling_broker: Broker) -> None:
    def _fast_task() -> int:
        return 99

    tracker = StatusTracker(rolling_broker.backend)

    fast_task = task(
        rolling_broker,
        name="fast_task",
        mechanism="thread",
        max_retries=0,
        status_tracker=tracker,
    )(_fast_task)

    msg = fast_task.generate()

    pool = ThreadWorkerPool(tasks=[fast_task], n_threads=1, n_processes=1)

    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    # Actively wait for the status to change
    tracker.wait_for(
        message=msg,
        status=TERMINAL_TASK_STATUSES,
        timeout=5,
    )

    # now shutdown pool
    pool.stop_signal.exit_event.set()
    runner.join(timeout=2)

    assert tracker.get(msg).status == TaskStatus.SUCCEEDED



# ------------------------------------------------------------------ #
# 10. Capacity is never exceeded
# ------------------------------------------------------------------ #


@pytest.mark.timeout(30)
@pytest.mark.flaky(max_runs=2)
def test_pool_never_exceeds_capacity(rolling_broker) -> None:
    """
    Spin up a pool (2 procs × 2 threads = 4 max) and submit 12 tasks that each
    sleep for 50 ms.  We track the *peak* number of concurrent executions with
    a multiprocessing.Value shared between all workers.
    """
    manager = mp.Manager()
    current = manager.Value("i", 0)
    peak = manager.Value("i", 0)
    lock = manager.Lock()

    def _tracked_work() -> None:
        with lock:
            current.value += 1
            peak.value = max(peak.value, current.value)
        time.sleep(0.05)
        with lock:
            current.value -= 1

    tracked_task = task(
        rolling_broker,
        name="tracked",
        mechanism="thread",
        timeout=500,
    )(_tracked_work)

    pool = ThreadWorkerPool(
        tasks=[tracked_task],
        n_threads=2,
        n_processes=2,
    )

    # create > capacity messages
    for _ in range(12):
        tracked_task.generate()

    # run pool in background until queue drained
    t = threading.Thread(target=pool.run, daemon=True)
    t.start()
    # wait until all queues empty or timeout
    deadline = time.time() + 4
    while (
        any(_queue_has_items(g.queue) for g in pool._progress_groups)
        and time.time() < deadline
    ):
        time.sleep(0.02)

    pool._can_run = False  # stop engine loop
    t.join(timeout=1)
    pool.on_shutdown()

    assert peak.value <= 4, f"peak concurrency {peak.value} exceeded capacity 4"


# ------------------------------------------------------------------ #
# 11. Per-message timeout triggers retry/fail path
# ------------------------------------------------------------------ #

# @pytest.mark.timeout(15)
# def test_task_timeout_causes_retry(rolling_broker: Broker) -> None:
#     def _slow() -> None:
#         time.sleep(0.2)
#
#     slow_task = task(
#         rolling_broker,
#         name="slow",
#         mechanism="thread",
#         timeout=50,  # ms
#         max_retries=-1,  # <— *do not* re-queue after timeout
#     )(_slow)
#
#     msg = slow_task.generate()
#
#     pool = ThreadWorkerPool(tasks=[slow_task], n_threads=1, n_processes=1)
#     pool.run()
#
#     status = StatusTracker(rolling_broker).get(msg)
#     assert status == "FAILED"


# ------------------------------------------------------------------ #
# 12. Graceful shutdown stops children
# ------------------------------------------------------------------ #


@pytest.mark.timeout(30)
def test_graceful_shutdown_cleans_processes(rolling_broker: Broker) -> None:
    """
    Run a pool in a background thread, then set its stop-signal.
    All ProcessGroup processes must exit within 1 s.
    """
    stopper = threading.Event()

    def _work() -> None:
        while not stopper.is_set():
            time.sleep(0.01)

    busy_task = task(
        rolling_broker,
        name="busy",
        mechanism="thread",
        timeout=10_000,
    )(_work)

    # Generate a handful so at least one thread per process is alive
    for _ in range(6):
        busy_task.generate()

    pool = ThreadWorkerPool(tasks=[busy_task], n_threads=2, n_processes=2)
    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    time.sleep(0.1)  # let workers spin up
    pool.stop_signal.exit_event.set()  # request shutdown
    stopper.set()  # allow tasks to finish quickly
    runner.join(timeout=2)

    # All child processes must be dead
    assert all(not g.process.is_alive() for g in pool._progress_groups)


# ------------------------------------------------------------------ #
# 13. Revocation mid-flight is handled (bonus)
# ------------------------------------------------------------------ #


@pytest.mark.timeout(30)
def test_revocation_mid_flight(
    rolling_broker: Broker,
    tmp_path: Path,
) -> None:
    """
    The task writes to a file after 100 ms.  We revoke mid-flight and expect the
    file to *not* exist because the future is stopped.
    """
    outfile = tmp_path / "should_not_exist.txt"

    def _writes() -> None:
        time.sleep(0.1)
        outfile.write_text("oops")

    revocable_task = task(
        rolling_broker,
        name="revocable",
        mechanism="thread",
        timeout=500,
    )(_writes)

    msg = revocable_task.generate()

    pool = ThreadWorkerPool(tasks=[revocable_task], n_threads=1, n_processes=1)
    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    time.sleep(0.03)  # give it a moment to start
    revocable_task.revoke(msg)  # send revocation
    runner.join(timeout=1)
    pool.on_shutdown()

    assert not outfile.exists()
