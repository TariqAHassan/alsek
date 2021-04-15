"""

    Test Temporal

"""
import pytest

from alsek._utils.temporal import time_ms, utcfromtimestamp_ms, utcnow_timestamp_ms


def test_utcnow_timestamp_ms() -> None:
    assert isinstance(utcnow_timestamp_ms(), int)


def test_utcfromtimestamp_ms() -> None:
    ts = utcnow_timestamp_ms()
    dt = utcfromtimestamp_ms(ts)
    assert int(dt.timestamp() * 1000) == ts


def test_time_ms() -> None:
    assert isinstance(time_ms(), int)
