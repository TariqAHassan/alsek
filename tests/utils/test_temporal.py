"""

    Test Temporal

"""

import pytest

from alsek.utils.temporal import from_timestamp_ms, time_ms, utcnow_timestamp_ms


def test_utcnow_timestamp_ms() -> None:
    assert isinstance(utcnow_timestamp_ms(), int)


def test_from_timestamp_ms() -> None:
    ts = utcnow_timestamp_ms()
    dt = from_timestamp_ms(ts)
    assert int(dt.timestamp() * 1000) == ts


def test_time_ms() -> None:
    assert isinstance(time_ms(), int)
