"""

    Waiting

"""
from typing import Callable, Optional

from alsek._utils.system import StopSignalListener
from alsek._utils.temporal import time_ms


def waiter(
    condition: Callable[[], bool],
    sleep_interval: int = 1 * 1000,
    timeout: Optional[int] = None,
    timeout_msg: Optional[str] = None,
) -> bool:
    """Wait for ``condition``.

    Args:
        condition (callable): condition to wait for
        sleep_interval (int): time (in milliseconds) to sleep
            between checks of ``condition``.
        timeout (int, optional): maximum amount of time (in milliseconds)
            this function can wait for ``condition`` to evaluate
            to ``True``.
        timeout_msg (str, optional): message to display in the
            event of a timeout

    Returns:
        bool

    """
    start = time_ms()
    stop_signal = StopSignalListener()
    while True:
        if stop_signal.received:
            return False
        elif condition():
            return True
        elif timeout is not None and (time_ms() - start) > timeout:
            raise TimeoutError(timeout_msg or "")
        else:
            stop_signal.wait(min(sleep_interval, timeout or float("inf")))
