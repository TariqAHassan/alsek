"""

    Test Concurrency

"""

import multiprocessing as mp
import threading
from queue import Queue
from typing import Any
import os

import dill
import pytest
import redis_lock

from alsek.core.concurrency import Lock, ProcessLock, ThreadLock
from alsek.storage.backends import Backend


def test_repr(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    assert isinstance(repr(lock), str)


@pytest.mark.parametrize("do_acquire", [True, False])
def test_holder(do_acquire: bool, rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)

    if do_acquire:
        lock.acquire()
        assert lock.holder == lock.owner_id
    else:
        assert lock.holder is None


@pytest.mark.parametrize("do_acquire", [True, False])
def test_held(do_acquire: bool, rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)

    if do_acquire:
        lock.acquire()
        assert lock.held
    else:
        assert not lock.held


def test_acquire(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    lock.acquire()
    assert lock.held


def test_multi_acquire(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    assert lock.acquire()
    assert lock.acquire(if_already_acquired="return_true")

    with pytest.raises(redis_lock.AlreadyAcquired):
        lock.acquire(if_already_acquired="raise_error")


def test_release(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    lock.acquire()
    assert lock.held
    lock.release()
    assert not lock.held


@pytest.mark.parametrize("auto_release", [True, False])
def test_context(auto_release: bool, rolling_backend: Backend) -> None:
    with Lock("lock", backend=rolling_backend, auto_release=auto_release) as lock:
        lock.acquire()
        assert lock.held

    if auto_release:
        assert not lock.held
    else:
        assert lock.held


def _rebuild_backend(encoded: bytes) -> Backend:
    data: dict[str, Any] = dill.loads(encoded)
    backend_cls = data["backend"]
    return backend_cls._from_settings(data["settings"])  # noqa


def test_owner_id_isolation(rolling_backend: Backend) -> None:
    """Test that Lock provides process-level isolation."""
    lock_1 = Lock("test", backend=rolling_backend, owner_id="A")
    lock_2 = Lock("test", backend=rolling_backend, owner_id="A")
    lock_3 = Lock("test", backend=rolling_backend, owner_id="B")

    # Acquire with the first lock object
    assert lock_1.acquire()
    # Release with the second lock object
    assert lock_2.release()
    assert not lock_2.release()
    # Acquire with the second lock
    assert lock_2.acquire()
    # Try to release with the third lock
    # This should fail because the owner_id is different.
    assert not lock_3.release()


def test_host_level_lock(rolling_backend: Backend) -> None:
    """Test that standard Lock shares owner_id across instances on the same host."""
    lock_name = "host_lock"
    primary_lock = Lock(lock_name, backend=rolling_backend)
    secondary_lock = Lock(lock_name, backend=rolling_backend)

    # Both locks should have the same owner_id since they're on the same host
    assert primary_lock.owner_id == secondary_lock.owner_id

    # Acquire the lock with first instance
    assert primary_lock.acquire()
    assert primary_lock.held

    # The second instance should also see the lock as held
    assert secondary_lock.held

    # Second instance should be able to reacquire with already_acquired_ok=True
    assert secondary_lock.acquire(if_already_acquired="return_true")

    # Either instance should be able to release
    assert secondary_lock.release()
    assert not primary_lock.held

    # Clean-up
    rolling_backend.clear_namespace()


def test_thread_isolation(rolling_backend: Backend) -> None:
    """Test that ThreadLock provides thread-level isolation."""
    lock_name = "thread_lock"
    primary_lock = ThreadLock(lock_name, backend=rolling_backend)

    # Thread-0 takes the lock
    assert primary_lock.acquire()
    assert primary_lock.held

    # Serialise the backend so the worker thread can build its own copy
    encoded_backend = rolling_backend.encode()
    q: Queue[bool] = Queue()

    def worker() -> None:
        secondary_backend = _rebuild_backend(encoded_backend)
        acquired = ThreadLock(lock_name, backend=secondary_backend).acquire()
        q.put(acquired)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join()

    # The second thread must *not* obtain the lock
    assert q.get() is False

    # Clean-up
    assert primary_lock.release()
    primary_lock.backend.clear_namespace()


def _worker_process_lock(name: str, encoded_backend: bytes, q: Queue) -> None:
    backend = _rebuild_backend(encoded_backend)
    got_lock = ProcessLock(name, backend=backend).acquire()
    q.put(got_lock)


def test_process_isolation(rolling_backend: Backend) -> None:
    """Test that ProcessLock provides process-level isolation."""
    lock_name = "process_lock"
    lock = ProcessLock(lock_name, backend=rolling_backend)

    assert lock.acquire()

    result_q: Queue[bool] = mp.Queue()
    proc = mp.Process(
        target=_worker_process_lock,
        args=(lock_name, rolling_backend.encode(), result_q),
        daemon=True,
    )
    proc.start()
    proc.join()

    # The child process must *not* acquire the lock
    assert result_q.get() is False

    # Clean-up
    assert lock.release()
    lock.backend.clear_namespace()


def test_process_lock(rolling_backend: Backend) -> None:
    """Test that ProcessLock uses process-specific owner_id."""
    lock = ProcessLock("process_lock", backend=rolling_backend)
    assert "lock:" in lock.owner_id
    assert str(os.getpid()) in lock.owner_id

    assert lock.acquire()
    assert lock.held
    assert lock.release()


def test_thread_lock(rolling_backend: Backend) -> None:
    """Test that ThreadLock uses thread-specific owner_id."""
    lock = ThreadLock("thread_lock", backend=rolling_backend)
    assert "lock:" in lock.owner_id
    assert str(os.getpid()) in lock.owner_id
    assert str(threading.get_ident()) in lock.owner_id

    assert lock.acquire()
    assert lock.held
    assert lock.release()
