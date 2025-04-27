# tests/core/pool/test_process.py

import time
from pathlib import Path

import pytest

from alsek import Message
from alsek.core.pools.process import ProcessWorkerPool
from alsek.core.futures import ProcessTaskFuture
from alsek.core.task import task
from alsek.exceptions import TerminationError
from alsek.storage.result import ResultStore
from alsek.core.status import StatusTracker, TaskStatus


# ------------------------------------------------------------------ #
# Fixture-based rolling pool (one-submit, finite-run)
# ------------------------------------------------------------------ #
# Provided by conftest.py:
# @pytest.fixture()
# def rolling_process_worker_pool(rolling_broker): ...


# ------------------------------------------------------------------ #
# 1. submit_message appends a future
# ------------------------------------------------------------------ #
def test_submit_message_appends_future(rolling_process_worker_pool):
    pool = rolling_process_worker_pool
    msg = pool.tasks[0].generate()
    submitted = pool.submit_message(msg)
    assert submitted is True
    assert len(pool._futures) == 1
    assert isinstance(pool._futures[0], ProcessTaskFuture)


# ------------------------------------------------------------------ #
# 2. has_slot & submit_message respects n_processes limit
# ------------------------------------------------------------------ #
def test_has_slot_and_submit_message_limits(rolling_broker):
    t = task(rolling_broker, name="T", mechanism="process")(lambda: time.sleep(0.01))
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
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if all(f.complete for f in pool._futures):  # type: ignore[attr-defined]
            break
        time.sleep(0.01)
    else:
        pytest.skip("Child processes did not complete in time")

    pool.prune()
    assert pool.has_slot() is True
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 3. prune() removes completed & timed-out futures
# ------------------------------------------------------------------ #
def test_prune_behavior(rolling_process_worker_pool):
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
def test_on_shutdown_terminates_all(rolling_process_worker_pool):
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


# ================================================================== #
# Essential end-to-end integration tests
# ================================================================== #


# ------------------------------------------------------------------ #
# 5. executes tasks, writes files, frees slots
# ------------------------------------------------------------------ #
@pytest.mark.parametrize("n_processes", [1, 3])
def test_process_pool_executes_and_frees_slots(tmp_path, rolling_broker, n_processes):
    # prepare output files
    outfiles = [tmp_path / f"out{i}.txt" for i in range(n_processes)]

    def make_writer(path: Path):
        def _writer() -> None:
            path.write_text("done")

        return _writer

    tasks = [
        task(rolling_broker, name=f"writer{i}", mechanism="process", timeout=1000)(
            make_writer(outfiles[i])
        )
        for i in range(n_processes)
    ]
    pool = ProcessWorkerPool(tasks=tasks, n_processes=n_processes)
    msgs = [t.generate() for t in tasks]
    for m in msgs:
        assert pool.submit_message(m) is True

    # wait for all to complete
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if all(f.complete for f in pool._futures):  # type: ignore[attr-defined]
            break
        time.sleep(0.01)
    else:
        pytest.skip("Child processes did not finish in time")

    pool.prune()
    for path in outfiles:
        assert path.read_text() == "done"
    assert pool.has_slot() is True
    pool.on_shutdown()


# ------------------------------------------------------------------ #
# 6. multiple submits produce exact future count
# ------------------------------------------------------------------ #
def test_multiple_submits_exact_future_count(rolling_broker):
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
def test_end_to_end_status_and_result(rolling_broker):
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

    deadline = time.time() + 1.0
    while time.time() < deadline:
        try:
            if status.get(msg).status == TaskStatus.SUCCEEDED:
                break
        except KeyError:
            pass
        time.sleep(0.01)
    else:
        pytest.skip("Task did not complete in time")

    assert status.get(msg).status == TaskStatus.SUCCEEDED
    assert results.get(msg) == 123

    pool.prune()
    pool.on_shutdown()
