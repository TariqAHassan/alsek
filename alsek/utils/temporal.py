"""

    Temporal Utils

"""

import time
from datetime import datetime, timezone


def utcnow() -> datetime:
    """Generates the current UTC datetime without timezone information.

    This function retrieves the current date and time in UTC. The resulting
    datetime object will have its timezone information stripped by replacing
    it with None. This operation ensures that the resulting datetime is naive.

    Returns:
        datetime: The current UTC date and time, with no timezone information.

    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utcnow_timestamp_ms() -> int:
    """UTC timestamp in milliseconds.

    Returns:
        timestamp (int): UTC time in milliseconds

    """
    return int(utcnow().timestamp() * 1000)


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
