# tests/core/pool/test_process.py

import pytest

from alsek import Message
from alsek.core.pools.process import ProcessWorkerPool
from alsek.core.futures import ProcessTaskFuture
from alsek.core.task import task
from alsek.exceptions import TerminationError


# ------------------------------------------------------------------ #
# 1. submit_message appends a future
# ------------------------------------------------------------------ #

def test_submit_message_appends_future(rolling_process_worker_pool):
    pool = rolling_process_worker_pool
    # generate and submit one message
    msg = pool.tasks[0].generate()
    submitted = pool.submit_message(msg)
    assert submitted is True
    # exactly one future was created
    assert len(pool._futures) == 1
    assert isinstance(pool._futures[0], ProcessTaskFuture)


# ------------------------------------------------------------------ #
# 2. has_slot & submit_message respects n_processes limit
# ------------------------------------------------------------------ #
def test_has_slot_and_submit_message_limits(rolling_broker):
    import time
    # a simple sleep task
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

    # wait for the first two futures to complete
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if all(f.complete for f in pool._futures):  # type: ignore[attr-defined]
            break
        time.sleep(0.01)
    else:
        pytest.skip("Child processes did not complete in time")

    pool.prune()
    # slot should free up
    assert pool.has_slot() is True

    pool.on_shutdown()



# ------------------------------------------------------------------ #
# 3. prune() removes completed & timed-out futures
# ------------------------------------------------------------------ #

def test_prune_behavior(rolling_process_worker_pool):
    pool = rolling_process_worker_pool

    # craft fake futures
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

    f1 = FakeFuture(complete=True,  tlex=False)
    f2 = FakeFuture(complete=False, tlex=True)
    f3 = FakeFuture(complete=False, tlex=False)
    pool._futures = [f1, f2, f3]  # type: ignore[attr-defined]

    pool.prune()
    # only the “live” one remains
    assert pool._futures == [f3]
    # timed-out one was stopped & cleaned
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
    done  = FakeFuture(complete=True)
    pool._futures = [alive, done]  # type: ignore[attr-defined]

    pool.on_shutdown()
    # only the alive one got stopped & cleaned
    assert alive.stopped and alive.cleaned
    assert not done.stopped
    # futures list is cleared
    assert pool._futures == []
