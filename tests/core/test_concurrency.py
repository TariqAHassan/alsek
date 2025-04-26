"""

    Test Concurrency

"""

from socket import gethostname
import os
import multiprocessing
import threading
from queue import Queue

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


def test_thread_isolation(rolling_backend: Backend) -> None:
    """Test that a lock acquired in one thread cannot be acquired in another thread."""
    lock = Lock("thread_lock", backend=rolling_backend)
    assert lock.acquire()
    assert lock.held

    result_queue = Queue()

    def thread_func() -> None:
        # Try to acquire the same lock in a different thread
        thread_lock = Lock("thread_lock", backend=rolling_backend)
        acquire_result = thread_lock.acquire()
        result_queue.put(acquire_result)

    thread = threading.Thread(target=thread_func)
    thread.start()
    thread.join()

    # The thread should not be able to acquire the lock
    assert not result_queue.get()

    # Clean up
    lock.release()
