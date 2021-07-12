"""

    System Utils

"""
import ctypes
import logging
import signal
import sys
from multiprocessing import cpu_count
from signal import Signals
from threading import Event
from typing import Any, Optional, Tuple, Type, Union

log = logging.getLogger(__name__)

# See https://docs.python.org/3/c-api/init.html#c.PyThreadState_SetAsyncExc
_SET_AS_ASYNC_EXC_SIGNATURE_CHANGE: float = 37.0
DEFAULT_STOP_SIGNALS: Tuple[int, ...] = (signal.SIGTERM, signal.SIGINT)


def _numeric_python_version() -> float:
    return (sys.version_info[0] * 10) + sys.version_info[1] + (sys.version_info[2] / 10)


def _cast_ident_to_ctype(ident: int) -> Union[ctypes.c_long, ctypes.c_ulonglong]:
    if _numeric_python_version() >= _SET_AS_ASYNC_EXC_SIGNATURE_CHANGE:
        return ctypes.c_ulonglong(ident)
    else:
        return ctypes.c_long(ident)


def thread_raise(ident: int, exception: Type[BaseException]) -> None:
    """Raise an exception in a thread asynchronously.

    Args:
        ident (int): ident of the thread
        exception (Type[BaseException]): type of exception to raise in the thread

    References:
        * https://docs.python.org/3/c-api/init.html#c.PyThreadState_SetAsyncExc

    Warning:
        * Intended for use with CPython only

    """
    n = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        _cast_ident_to_ctype(ident),
        ctypes.py_object(exception),
    )
    if n != 1:
        log.warning(f"Raising {exception} in thread {ident} modified {n} threads")


class StopSignalListener:
    """Tool for listing for stop signals.

    Args:
        stop_signals (Tuple[int, ...], optional): one or more stop
            signals to listen for.
        exit_override (bool): trigger an immediate and non-graceful shutdown
            of the current process if two or more SIGTERM or SIGINT signals
            are received.

    """

    def __init__(
        self,
        stop_signals: Tuple[int, ...] = DEFAULT_STOP_SIGNALS,
        exit_override: bool = True,
    ) -> None:
        self.stop_signals = stop_signals
        self.exit_override = exit_override

        self.exit_event = Event()
        for s in self.stop_signals:
            signal.signal(s, self._signal_handler)

    def _signal_handler(self, signum: int, *args: Any) -> None:  # noqa
        log.debug("Received stop signal %s...", Signals(signum).name)
        if self.exit_override and self.received:
            sys.exit(1)
        self.exit_event.set()

    def wait(self, timeout: Optional[int]) -> None:
        """Wait for a stop signal to be received.

        Args:
            timeout (int, optional): amount of time
                (in milliseconds) to wait

        Returns:
            None

        """
        self.exit_event.wait(timeout if timeout is None else timeout / 1000)

    @property
    def received(self) -> bool:
        """Whether or not a stop signal has been received."""
        return self.exit_event.is_set()


def smart_cpu_count() -> int:
    """Count the number of CPUs, with one reserved
    for the main process.

    Returns:
        count (int): number of cpus

    """
    return max(1, cpu_count() - 1)
