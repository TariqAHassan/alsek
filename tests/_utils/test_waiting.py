"""

    Test Waiting

"""
from typing import Any, Optional

import pytest

from alsek._utils.temporal import time_ms
from alsek._utils.waiting import waiter


@pytest.mark.parametrize(
    "condition_time,timeout,exception",
    [
        (100, 5000, None),
        (1000, 250, TimeoutError),
    ],
)
def test_waiter(
    condition_time: int,
    timeout: int,
    exception: Optional[TimeoutError],
) -> None:
    start = time_ms()

    def run():
        waiter(
            condition=lambda: (time_ms() - start) > condition_time,
            sleep_interval=1,
            timeout=timeout,
            timeout_msg="Timeout",
        )

    if exception is None:
        run()
        assert (time_ms() - start) > condition_time
    else:
        with pytest.raises(exception):
            run()
