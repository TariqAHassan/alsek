"""

    Test Concurrency

"""
import pytest
from socket import gethostname
from alsek.core.concurrency import Lock
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
        assert lock.holder == gethostname()
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
