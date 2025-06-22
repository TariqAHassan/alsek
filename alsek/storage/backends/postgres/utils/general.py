"""

    Utils

"""

from __future__ import annotations

POSTGRES_MAX_NOTIFICATION_SIZE_LIMIT: int = 8_000


def validate_value_within_postgres_notification_size_limit(value: str | bytes) -> None:
    if isinstance(value, str):
        n_bytes = len(value.encode("utf-8"))
    elif isinstance(value, bytes):
        n_bytes = len(value)
    else:
        raise TypeError(f"Invalid input type {type(value)}")

    if n_bytes > POSTGRES_MAX_NOTIFICATION_SIZE_LIMIT:
        raise ValueError(
            "Message payload too large for PostgreSQL NOTIFY (max 8000 bytes)"
        )
