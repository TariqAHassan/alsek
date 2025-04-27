"""

    Test Thread Pool

"""

from __future__ import annotations

import multiprocessing as mp
import queue as _q
import threading
import time
from multiprocessing import Event
from multiprocessing import Queue as MPQueue
from pathlib import Path
from queue import Queue
from typing import Any

import pytest
import dill

from alsek import Broker, Message, StatusTracker
from alsek.core.status import TERMINAL_TASK_STATUSES, TaskStatus
from alsek.core.task import task
from alsek.core.worker.thread import (
    ProcessGroup,
    ThreadsInProcessGroup,
    ThreadWorkerPool,
)
from alsek.exceptions import RevokedError
from alsek.storage.result import ResultStore

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
        slot_wait_interval_seconds=0.01,
    )
    assert g.has_slot() is True
    # fill the queue with dummy serialized data
    dummy_payload = dill.dumps(({}, {}, False))
    g.queue.put(dummy_payload)
    g.queue.put(dummy_payload)
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
        slot_wait_interval_seconds=0.01,
    )
    # ensure put() doesn't raise so we hit the happy path
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
        slot_wait_interval_seconds=0.01,
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
        slot_wait_interval_seconds=0.01,
    )
    assert g.process.is_alive()
    g.stop()
    assert not g.process.is_alive()


# ------------------------------------------------------------------ #
# 7. ThreadInProcessGroup._has_capacity()
# ------------------------------------------------------------------ #


def test_threads_in_process_group_has_capacity() -> None:
    tig = ThreadsInProcessGroup(
        q=MPQueue(),
        shutdown_event=Event(),
        n_threads=2,
        slot_wait_interval_seconds=0.01,
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
        slot_wait_interval_seconds=0.01,
    )
    pool._progress_groups.append(dead_group)
    dead_group.stop(timeout=0.05)
    time.sleep(0.05)  # allow OS to reap

    pool.prune()
    assert dead_group not in pool._progress_groups


# ------------------------------------------------------------------ #
# 9. ThreadInProcessGroup._prune() drops finished & timed-out futures
# ------------------------------------------------------------------ #


def test_threads_in_process_group_prune_behavior() -> None:
    """• remove objects whose .complete is True
    • kill + drop objects whose .time_limit_exceeded is True
    • keep live, incomplete futures"""
    tig = ThreadsInProcessGroup(
        q=MPQueue(),
        shutdown_event=Event(),
        n_threads=10,
        slot_wait_interval_seconds=0.01,
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

    tig._live = [live, finished, timed_out]  # noqa
    tig._prune()

    # finished & timed-out should be gone; live remains
    assert tig._live == [live]
    # timed_out future must have been stopped & cleaned
    assert timed_out.stop_called is True and timed_out.cleaned is True


# ------------------------------------------------------------------ #
# 10-12. ThreadWorkerPool._acquire_group() logic
# ------------------------------------------------------------------ #


def _mk_pool(broker: Broker) -> ThreadWorkerPool:
    """Spin up a tiny pool with a single dummy thread-task.
    Configures the pool to use ._poll() instead of .stream()
    so it doesn't run indefinitely if .run() were called.
    """
    from alsek.core.task import task

    # Need a real task definition for the pool itself
    dummy_task_for_pool = task(broker, name="dummy_pool_task", mechanism="thread")(lambda: None)
    pool = ThreadWorkerPool(
        tasks=[dummy_task_for_pool],
        n_threads=1,
        n_processes=2,  # small cap so we can hit it quickly
        # Use a short interval for faster test execution if needed, but default might be fine
        # slot_wait_interval_seconds=0.01
    )
    # Make the stream finite for testing like in conftest.py
    pool.stream = pool._poll 
    return pool


def test_acquire_group_reuses_available_slot(rolling_broker: Broker) -> None:
    pool = _mk_pool(rolling_broker)
    g1 = pool._acquire_group()  # spawns first group
    assert g1 is not None
    # queue still empty → should reuse
    assert pool._acquire_group() is g1
    # Stop the group process AFTER the test assertion
    g1.stop(timeout=0.1) 
    pool.on_shutdown()


def test_acquire_group_spawns_until_cap(rolling_broker: Broker, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _mk_pool(rolling_broker)

    g1 = pool._acquire_group()
    assert g1 is not None
    assert len(pool._progress_groups) == 1
    # Don't stop g1 process

    # Simulate g1 being full using monkeypatch
    monkeypatch.setattr(g1, 'has_slot', lambda: False)

    g2 = pool._acquire_group()  # new spawn because g1 reports no slot
    assert g2 is not None
    assert g2 is not g1
    assert len(pool._progress_groups) == 2
    # Don't stop g2 process
    
    # Simulate g2 also being full using monkeypatch
    monkeypatch.setattr(g2, 'has_slot', lambda: False)

    # Now, with g1 and g2 mocked as full, acquire should fail as pool capacity (2) is met
    assert pool._acquire_group() is None

    # Cleanup - stop the original groups
    pool.on_shutdown() # This will call the original stop methods


@pytest.mark.timeout(10)
def test_submit_message_fails_when_every_group_full(rolling_broker: Broker, monkeypatch: pytest.MonkeyPatch) -> None:
    pool = _mk_pool(rolling_broker)

    # Acquire first group
    g1 = pool._acquire_group()
    assert g1 is not None
    # IMMEDIATELY simulate it being full
    monkeypatch.setattr(g1, 'has_slot', lambda: False)

    # Acquire second group (should be new since g1 is mocked as full)
    g2 = pool._acquire_group()
    assert g2 is not None
    assert g1 is not g2 # This should now pass
    # IMMEDIATELY simulate it being full
    monkeypatch.setattr(g2, 'has_slot', lambda: False)

    # generate a real Message object from the pool's task to pass in
    msg = pool.tasks[0].generate()
    # submit_message calls _acquire_group, which will check g1/g2.has_slot() (mocked)
    # Since both return False, and len(groups) == n_processes, _acquire_group returns None
    # Therefore, submit_message should return False
    assert pool.submit_message(msg) is False

    # Cleanup - stop the original groups
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 9. Normal FLow
# ------------------------------------------------------------------ #


@pytest.mark.timeout(10)
@pytest.mark.flaky(max_runs=2)
def test_task_completes_successfully(rolling_broker: Broker) -> None:
    def _fast_task() -> int:
        return 99

    status_tracker = StatusTracker(rolling_broker.backend)
    result_store = ResultStore(rolling_broker.backend)

    fast_task = task(
        rolling_broker,
        name="fast_task",
        mechanism="thread",
        max_retries=0,
        status_tracker=status_tracker,
        result_store=result_store,
    )(_fast_task)

    msg = fast_task.generate()

    pool = ThreadWorkerPool(tasks=[fast_task], n_threads=1, n_processes=1)

    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    # Actively wait for the status to change
    status_tracker.wait_for(
        message=msg,
        status=TERMINAL_TASK_STATUSES,
        timeout=5,
    )

    # now shutdown pool
    pool.stop_signal.exit_event.set()
    runner.join(timeout=2)

    assert status_tracker.get(msg).status == TaskStatus.SUCCEEDED
    assert result_store.get(msg) == 99


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


@pytest.mark.timeout(15)
@pytest.mark.flaky(max_runs=2)
def test_task_timeout_causes_retry(rolling_broker: Broker) -> None:
    def _slow() -> None:
        time.sleep(0.2)

    status_tracker = StatusTracker(rolling_broker.backend)

    slow_task = task(
        rolling_broker,
        name="slow",
        mechanism="thread",
        timeout=50,  # ms — timeout after 50ms
        status_tracker=status_tracker,
        max_retries=-1,
    )(_slow)

    msg = slow_task.generate()

    pool = ThreadWorkerPool(tasks=[slow_task], n_threads=1, n_processes=1)
    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    # Wait until it fails
    status_tracker.wait_for(
        message=msg,
        status=TaskStatus.FAILED,
        timeout=5,
    )

    pool.stop_signal.exit_event.set()
    runner.join(timeout=2)

    assert status_tracker.get(msg).status == TaskStatus.FAILED


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
@pytest.mark.flaky(max_runs=3)
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
        time.sleep(10)
        outfile.write_text("oops")

    status_tracker = StatusTracker(rolling_broker.backend)

    revocable_task = task(
        rolling_broker,
        name="revocable",
        mechanism="thread",
        timeout=500,
        status_tracker=status_tracker,
    )(_writes)

    msg = revocable_task.generate()

    pool = ThreadWorkerPool(tasks=[revocable_task], n_threads=1, n_processes=1)
    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    assert status_tracker.wait_for(
        msg,
        (TaskStatus.SUBMITTED, TaskStatus.RUNNING),
        timeout=10,
    )
    revocable_task.revoke(msg)  # send revocation
    runner.join(timeout=3)
    pool.on_shutdown()

    assert not outfile.exists()
    assert status_tracker.wait_for(msg, TaskStatus.FAILED, timeout=10)
    assert rolling_broker.sync(msg).exception_details
    assert isinstance(
        rolling_broker.sync(msg).exception_details.as_exception(),
        RevokedError,
    )


# ------------------------------------------------------------------ #
# 14. Test Elastic Scaling
# ------------------------------------------------------------------ #


@pytest.mark.timeout(20)
@pytest.mark.flaky(max_runs=3)
def test_pool_elastic_scale_up_and_down(rolling_broker: Broker, monkeypatch):
    """
    Verify that ThreadWorkerPool spins up extra ProcessGroups under load,
    then prunes them back to zero once the queue is drained.
    """
    N_PROCESSES = 4  # noqa
    N_THREADS = 2  # noqa
    # anything > N_THREADS is enough to force a 2nd group
    N_JOBS = N_THREADS * (N_PROCESSES + 1)  # noqa

    # ---------- 1. shared counters ----------
    mgr = mp.Manager()
    concurrent = mgr.Value("i", 0)
    peak = mgr.Value("i", 0)
    lock = mgr.Lock()

    # ---------- 2. patch ProcessGroup ----------
    orig_pg_init = ProcessGroup.__init__

    def _patched_init(self, *a, **k):
        with lock:
            concurrent.value += 1
            peak.value = max(peak.value, concurrent.value)
        orig_pg_init(self, *a, **k)

    monkeypatch.setattr(ProcessGroup, "__init__", _patched_init, raising=False)

    orig_pg_stop = ProcessGroup.stop

    def _patched_stop(self, *a, **k):
        orig_pg_stop(self, *a, **k)
        with lock:
            concurrent.value -= 1

    monkeypatch.setattr(ProcessGroup, "stop", _patched_stop, raising=False)

    # ---------- 3. workload ----------
    def _work() -> None:
        time.sleep(0.05)

    work_task = task(
        rolling_broker,
        name="elastic",
        mechanism="thread",
        timeout=500,
    )(_work)

    pool = ThreadWorkerPool(
        tasks=[work_task],
        n_threads=N_THREADS,
        n_processes=N_PROCESSES,
    )

    # enqueue 40 jobs → more than a single ProcessGroup can hold
    for _ in range(N_JOBS):
        work_task.generate()

    t = threading.Thread(target=pool.run, daemon=True)
    t.start()

    # ---------- 4. wait until queue is empty ----------
    deadline = time.time() + 6
    while (
        any(_queue_has_items(g.queue) for g in pool._progress_groups)
        and time.time() < deadline
    ):
        time.sleep(0.05)

    # after queue drained …
    time.sleep(0.2)
    pool.prune()

    # ---------- 5. shutdown ----------
    pool.stop_signal.exit_event.set()
    t.join(timeout=2)
    pool.on_shutdown()

    # ---------- 6. assertions ----------
    assert peak.value > 1, "pool never scaled UP"
    assert concurrent.value < peak.value, "pool never scaled DOWN"
