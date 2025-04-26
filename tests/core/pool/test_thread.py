"""

    Test Thread Pool

"""
from queue import Queue

from alsek import Broker
from alsek.core.pools.thread import ThreadWorkerPool, ProcessGroup


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _futures_count(pool: ThreadWorkerPool) -> int:
    return sum(g.queue.qsize() for g in pool._progress_groups)


def _queue_has_items(q: Queue) -> bool:
    try:
        return q.qsize() > 0  # works on Linux / Windows
    except (NotImplementedError, AttributeError):
        return not q.empty()  # portable fallback


# ------------------------------------------------------------------ #
# 1. tasks consumed
# ------------------------------------------------------------------ #


def test_tasks_consumed(rolling_thread_worker_pool: ThreadWorkerPool) -> None:
    pool = rolling_thread_worker_pool

    # generate one message per task
    for t in pool.tasks:
        t.generate()

    pool.run()  # non-blocking because of fixture swap
    assert any(_queue_has_items(g.queue) for g in pool._progress_groups)


# ------------------------------------------------------------------ #
# 2–3. submit_message()
# ------------------------------------------------------------------ #

def test_submit_message_available_slot(rolling_thread_worker_pool: ThreadWorkerPool) -> None:
    pool = rolling_thread_worker_pool
    msg = pool.tasks[0].generate()
    assert pool.submit_message(msg) is True


def test_submit_message_pool_saturated(monkeypatch, rolling_thread_worker_pool) -> None:
    pool = rolling_thread_worker_pool

    # monkey-patch _acquire_group to always return None (no slots)
    monkeypatch.setattr(pool, "_acquire_group", lambda: None)

    msg = pool.tasks[0].generate()
    assert pool.submit_message(msg) is False


# ------------------------------------------------------------------ #
# 5. ProcessGroup.has_slot()
# ------------------------------------------------------------------ #

def test_process_group_has_slot(rolling_broker: Broker) -> None:
    # small direct check without full pool
    g = ProcessGroup(
        n_threads=2,
        complete_only_on_thread_exit=False,
        slot_wait_interval=0.01,
    )

    assert g.has_slot() is True
    # fill the underlying queue
    g.queue.put(b"x")
    g.queue.put(b"y")
    assert g.has_slot() is False
    g.stop()
