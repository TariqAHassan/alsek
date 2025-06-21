"""

    Temporal Utils

"""

import time
from datetime import datetime, timezone, timedelta
from typing import Optional


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


def from_timestamp_ms(timestamp: int) -> datetime:
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


def compute_expiry_datetime(
    current_time: datetime,
    ttl: Optional[int],
) -> Optional[datetime]:
    """Calculates the expiry datetime based on the provided time-to-live (TTL) in milliseconds
    and the current datetime.

    Args:
        current_time (datetime): current datetime used as a reference point
        ttl (int): The time-to-live in milliseconds. If `None`, no expiry is calculated.

    Returns:
        The calculated expiry datetime as a `datetime` object if `ttl` is not `None`,
        otherwise `None`.

    """
    if ttl is None:
        return None
    else:
        return current_time + timedelta(milliseconds=ttl)
