"""

    Temporal Utils

"""
import time
from datetime import datetime


def utcnow_timestamp_ms() -> int:
    """UTC timestamp in milliseconds.

    Returns:
        timestamp (int): UTC time in milliseconds

    """
    return int(datetime.utcnow().timestamp() * 1000)


def fromtimestamp_ms(timestamp: int) -> datetime:
    """Construct datetime object from UTC timestamp in milliseconds.

    Args:
        timestamp (int): UTC time in milliseconds

    Returns:
        datetime

    """
    return datetime.fromtimestamp(timestamp / 1000)


def time_ms() -> int:
    """Get the current time since the Epoch in milliseconds.

    Returns:
        time (int): current time in milliseconds

    """
    return int(time.time() * 1000)
