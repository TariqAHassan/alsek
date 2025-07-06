# tests/core/pool/test_process.py
import os
import threading
import time
from pathlib import Path

import pytest

from alsek import Broker
from alsek.core.futures import ProcessTaskFuture
from alsek.core.statuss.standard import StatusTracker, TaskStatus
from alsek.core.task import task
from alsek.core.worker.process import ProcessWorkerPool
from alsek.storage.result import ResultStore

# ------------------------------------------------------------------ #
# 1. submit_message appends a future
# ------------------------------------------------------------------ #


def test_submit_message_appends_future(
    rolling_process_worker_pool: ProcessWorkerPool,
) -> None:
    pool = rolling_process_worker_pool
    msg = pool.tasks[0].generate()
    submitted = pool.submit_message(msg)
    assert submitted is True
    assert len(pool._futures) == 1
    assert isinstance(pool._futures[0], ProcessTaskFuture)


# ------------------------------------------------------------------ #
# 2. has_slot & submit_message respects n_processes limit
# ------------------------------------------------------------------ #


def test_has_slot_and_submit_message_limits(rolling_broker: Broker) -> None:
    t = task(rolling_broker, name="T", mechanism="process")(lambda: time.sleep(0.1))
    pool = ProcessWorkerPool(tasks=[t], n_processes=2)
    msgs = [t.generate() for _ in range(3)]

    # fill both slots
    assert pool.has_slot() is True
    assert pool.submit_message(msgs[0]) is True
    assert pool.submit_message(msgs[1]) is True

    # now saturated
    assert pool.has_slot() is False
    assert pool.submit_message(msgs[2]) is False

    # wait for completion
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if all(f.complete for f in pool._futures):
            break
        time.sleep(0.01)

    pool.prune()
    assert pool.has_slot() is True
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 3. prune() removes completed & timed-out futures
# ------------------------------------------------------------------ #
def test_prune_behavior(rolling_process_worker_pool: ProcessWorkerPool) -> None:
    pool = rolling_process_worker_pool

    class FakeFuture:
        def __init__(self, *, complete, tlex):
            self.complete = complete
            self.time_limit_exceeded = tlex
            self.stopped = False
            self.cleaned = False

        def stop(self, exc):
            self.stopped = True

        def clean_up(self, ignore_errors=False):
            self.cleaned = True

    f1 = FakeFuture(complete=True, tlex=False)
    f2 = FakeFuture(complete=False, tlex=True)
    f3 = FakeFuture(complete=False, tlex=False)
    pool._futures = [f1, f2, f3]  # type: ignore[attr-defined]

    pool.prune()
    assert pool._futures == [f3]
    assert f2.stopped and f2.cleaned


# ------------------------------------------------------------------ #
# 4. on_shutdown() kills all live futures
# ------------------------------------------------------------------ #
def test_on_shutdown_terminates_all(
    rolling_process_worker_pool: ProcessWorkerPool,
) -> None:
    pool = rolling_process_worker_pool

    class FakeFuture:
        def __init__(self, *, complete):
            self.complete = complete
            self.stopped = False
            self.cleaned = False

        def stop(self, exc):
            self.stopped = True

        def clean_up(self, ignore_errors=False):
            self.cleaned = True

    alive = FakeFuture(complete=False)
    done = FakeFuture(complete=True)
    pool._futures = [alive, done]  # type: ignore[attr-defined]

    pool.on_shutdown()
    assert alive.stopped and alive.cleaned
    assert not done.stopped
    assert pool._futures == []


# ------------------------------------------------------------------ #
# 5. executes tasks, writes files, frees slots
# ------------------------------------------------------------------ #


# ToDo: resolve on GitHub CI
@pytest.mark.skip(reason="Buggy on GitHub CI")
@pytest.mark.parametrize("n_processes", [1, 3])
def test_process_pool_executes_and_frees_slots(
    tmp_path: Path,
    rolling_broker: Broker,
    n_processes: int,
) -> None:
    # prepare output files
    outfiles = [tmp_path / f"out{i}.txt" for i in range(n_processes)]

    def make_writer(path: Path):
        def _writer() -> None:
            # Ensure the parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("done")
            # Add sync to ensure file is written to disk
            if hasattr(os, "sync"):
                os.sync()

        return _writer

    tasks = [
        task(
            rolling_broker,
            name=f"writer{i}",
            mechanism="process",
            timeout=1000,
        )(make_writer(outfiles[i]))
        for i in range(n_processes)
    ]
    pool = ProcessWorkerPool(tasks=tasks, n_processes=n_processes)
    msgs = [t.generate() for t in tasks]
    for m in msgs:
        assert pool.submit_message(m) is True

    # wait for all to complete with a slightly longer timeout
    deadline = time.time() + 5.0
    while time.time() < deadline:
        pool.prune()  # Actively prune to remove completed futures
        if len(pool._futures) == 0:
            break
        time.sleep(0.05)

    # Double check all files exist before asserting content
    for path in outfiles:
        wait_time = 0
        while not path.exists() and wait_time < 4.0:
            time.sleep(0.1)
            wait_time += 0.1

        assert path.exists(), f"File {path} was not created"
        assert path.read_text() == "done"

    assert pool.has_slot() is True
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 6. multiple submits produce exact future count
# ------------------------------------------------------------------ #


def test_multiple_submits_exact_future_count(rolling_broker: Broker) -> None:
    t = task(rolling_broker, name="T", mechanism="process")(lambda: time.sleep(0.01))
    pool = ProcessWorkerPool(tasks=[t], n_processes=2)
    msgs = [t.generate() for _ in range(4)]
    results = [pool.submit_message(m) for m in msgs]
    assert results == [True, True, False, False]
    assert len(pool._futures) == 2
    time.sleep(0.05)
    pool.prune()
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 7. end-to-end status & result integration
# ------------------------------------------------------------------ #


def test_end_to_end_status_and_result(rolling_broker: Broker) -> None:
    status = StatusTracker(rolling_broker.backend)
    results = ResultStore(rolling_broker.backend)
    fast_task = task(
        rolling_broker,
        name="fast",
        mechanism="process",
        status_tracker=status,
        result_store=results,
        max_retries=0,
    )(lambda: 123)

    msg = fast_task.generate()
    pool = ProcessWorkerPool(tasks=[fast_task], n_processes=1)
    assert pool.submit_message(msg) is True

    assert status.wait_for(msg, TaskStatus.SUCCEEDED, timeout=5)
    assert status.get(msg).status == TaskStatus.SUCCEEDED
    assert results.get(msg) == 123

    pool.prune()
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 8. per-message timeout triggers FAILED status
# ------------------------------------------------------------------ #


# ToDo: resolve on GitHub CI
@pytest.mark.skip(reason="Buggy on GitHub CI")
@pytest.mark.flaky(max_runs=2)
def test_process_task_timeout_causes_failed_status(rolling_broker: Broker) -> None:
    backend = rolling_broker.backend
    status = StatusTracker(backend)

    def _slow() -> None:
        time.sleep(0.2)

    slow_task = task(
        rolling_broker,
        name="slow_proc",
        mechanism="process",
        timeout=50,  # ms
        max_retries=0,
        status_tracker=status,
    )(_slow)

    msg = slow_task.generate()
    pool = ProcessWorkerPool(
        tasks=[slow_task],
        n_processes=1,
        prune_interval=50,  # ms
    )
    assert pool.submit_message(msg) is True

    assert status.wait_for(msg, TaskStatus.FAILED, timeout=5)
    assert isinstance(
        rolling_broker.sync_from_backend(msg).exception_details.as_exception(), TimeoutError
    )


# ------------------------------------------------------------------ #
# 9. revocation mid-flight prevents side-effects
# ------------------------------------------------------------------ #


def test_process_revocation_mid_flight(rolling_broker: Broker, tmp_path: Path) -> None:
    outfile = tmp_path / "should_not_exist_proc.txt"

    def _writer() -> None:
        time.sleep(1)
        outfile.write_text("oops")

    status = StatusTracker(rolling_broker.backend)

    rev_task = task(
        rolling_broker,
        name="rev_proc",
        mechanism="process",
        timeout=500,
        status_tracker=status,
    )(_writer)

    msg = rev_task.generate()
    pool = ProcessWorkerPool(tasks=[rev_task], n_processes=1)

    # start pool in background
    runner = threading.Thread(target=pool.run, daemon=True)
    runner.start()

    time.sleep(0.03)
    rev_task.revoke(msg)
    assert status.wait_for(msg, TaskStatus.FAILED, timeout=5)

    runner.join(timeout=1)
    pool.prune()
    pool.on_shutdown()

    assert not outfile.exists()
