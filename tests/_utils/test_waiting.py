"""

    Test Waiting

"""
import time
from typing import Any, Optional

import pytest

from alsek._utils.waiting import waiter


@pytest.mark.parametrize(
    "condition_time,timeout,exception",
    [
        (500, 5000, None),
        (1000, 5000, None),
        (1000, 250, TimeoutError),
    ],
)
def test_waiter(
    condition_time: int,
    timeout: int,
    exception: Optional[BaseException],
) -> None:
    start = time.time()

    def run():
        waiter(
            condition=lambda: (time.time() - start) > condition_time,
            sleep_interval=500,
            timeout=timeout,
            timeout_msg="Timeout",
        )

    if isinstance(exception, BaseException):
        with pytest.raises(exception):
            run()
    else:
        assert (time.time() - start) < condition_time
