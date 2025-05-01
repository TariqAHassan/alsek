"""

    Test System

"""

import ctypes
import time
from multiprocessing import Process, Queue, cpu_count
from threading import Thread

import pytest

from alsek.utils.system import (
    StopSignalListener,
    _cast_ident_to_ctype,
    _numeric_python_version,
    smart_cpu_count,
    thread_raise,
)

_TIMEOUT: int = 1000
_FIRST_ITER_QUEUE: Queue = Queue()
_SIGNAL_QUEUE: Queue = Queue()


def _wait_for_signal(
    first_iter_queue: Queue,
    signal_queue: Queue,
) -> None:
    # Note: this must be defined here due to pickling limitations
    c = 0
    stop_signal = StopSignalListener()
    while True:
        if stop_signal.received:
            signal_queue.put(1)
            break
        if c == 0:
            first_iter_queue.put(1)
        c += 1
        stop_signal.wait(0.1)


def test_numeric_python_version() -> None:
    assert isinstance(_numeric_python_version(), float)


@pytest.mark.parametrize("ident", [1, 2, 3, 4, 5])
def test_cast_ident_to_ctype(ident: int) -> None:
    assert isinstance(
        _cast_ident_to_ctype(ident),
        (ctypes.c_long, ctypes.c_ulonglong),
    )


@pytest.mark.flaky(max_runs=3)
def test_thread_raise() -> None:
    thread_output = list()

    def no_stop() -> None:
        try:
            while True:
                time.sleep(0.1)
        except BaseException as error:
            thread_output.append(error)

    # Define and start the thread
    thread = Thread(target=no_stop, daemon=True)
    thread.start()

    # Raise a timeout error inside of it
    thread_raise(thread.ident, exception=TimeoutError)
    thread.join(timeout=10)

    # Ensure that the thread exited
    assert not thread.is_alive()

    # Ensure that what caused the exit was a timeout error
    assert thread_output and isinstance(thread_output[0], TimeoutError)


@pytest.mark.flaky(max_runs=5)
@pytest.mark.timeout(30)
def test_stop_signal_listener() -> None:
    start_time = time.time()
    process = Process(
        target=_wait_for_signal,
        args=(_FIRST_ITER_QUEUE, _SIGNAL_QUEUE),
        daemon=True,
    )
    process.start()

    # Wait for the first iteration to complete:
    while True:
        if not _FIRST_ITER_QUEUE.empty():
            break
        if (time.time() - start_time) > _TIMEOUT:
            raise TimeoutError("Timed out waiting for first iteration")

    # Send a stop signal to the process
    process.terminate()
    process.join(timeout=10)

    # Check that the signal was actually received inside the process
    assert not _SIGNAL_QUEUE.empty()


def test_smart_cpu_count() -> None:
    actual = smart_cpu_count()
    assert actual == max(1, cpu_count() - 1)
