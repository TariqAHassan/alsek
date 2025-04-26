"""

    Test Concurrency

"""

import multiprocessing as mp
import os
import threading
from queue import Queue
from socket import gethostname
from typing import Any

import dill
import pytest

from alsek.core.concurrency import Lock, _make_lock_name  # noqa
from alsek.storage.backends import Backend


def test_repr(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    assert isinstance(repr(lock), str)


def test_long_name(rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)
    assert lock.long_name == "locks:lock"


@pytest.mark.parametrize("do_acquire", [True, False])
def test_holder(do_acquire: bool, rolling_backend: Backend) -> None:
    lock = Lock("lock", backend=rolling_backend)

    if do_acquire:
        lock.acquire()
        assert lock.holder == lock._my_holder_id
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
    assert not lock.acquire()


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


def test_make_lock_name() -> None:
    lock_name = _make_lock_name()
    hostname = gethostname()
    pid = os.getpid()
    thread_id = threading.get_ident()
    expected_prefix = f"{hostname}:{pid}:{thread_id}"
    assert lock_name == expected_prefix


def _rebuild_backend(encoded: bytes) -> Backend:
    data: dict[str, Any] = dill.loads(encoded)
    backend_cls = data["backend"]
    return backend_cls._from_settings(data["settings"])  # noqa


def test_thread_isolation(rolling_backend: Backend) -> None:
    """Test that a lock acquired in one thread cannot be acquired in another thread."""
    lock_name = "thread_lock"
    primary_lock = Lock(lock_name, backend=rolling_backend)

    # Thread-0 takes the lock
    assert primary_lock.acquire(strict=True)
    assert primary_lock.held

    # Serialise the backend so the worker thread can build its own copy
    encoded_backend = rolling_backend.encode()
    q: Queue[bool] = Queue()

    def worker() -> None:
        secondary_backend = _rebuild_backend(encoded_backend)
        acquired = Lock(lock_name, backend=secondary_backend).acquire(strict=True)
        q.put(acquired)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join()

    # The second thread must *not* obtain the lock
    assert q.get() is False

    # Clean-up
    assert primary_lock.release()
    primary_lock.backend.clear_namespace()


def _worker(name: str, encoded_backend: bytes, q: Queue) -> None:
    backend = _rebuild_backend(encoded_backend)
    got_lock = Lock(name, backend=backend).acquire(strict=True)
    q.put(got_lock)


def test_process_isolation(rolling_backend: Backend) -> None:
    lock_name = "process_lock"
    lock = Lock(lock_name, backend=rolling_backend)

    assert lock.acquire(strict=True)

    result_q: Queue[bool] = mp.Queue()
    proc = mp.Process(
        target=_worker,
        args=(lock_name, rolling_backend.encode(), result_q),
        daemon=True,
    )
    proc.start()
    proc.join()

    assert result_q.get() is False  # child must *not* acquire the lock
    assert lock.release()
