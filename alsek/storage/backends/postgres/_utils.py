"""

    Utils

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, AsyncIterable

from sqlalchemy import URL

from alsek.storage.serialization import Serializer

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


class BasePostgresPubSubListen(ABC):
    def __init__(
        self,
        channel: str,
        url: URL,
        serializer: Serializer,
        sleep_time: float = 0.01,
    ) -> None:
        self.channel = channel
        self.url = url
        self.serializer = serializer
        self.sleep_time = sleep_time

    @staticmethod
    def _parse_notification_data(
        payload: str,
        serializer: Serializer,
    ) -> dict[str, Any]:
        data = serializer.reverse(payload)
        return {"type": "message", "data": data}

    @abstractmethod
    def listen(self) -> Iterable[Any] | AsyncIterable[Any]:
        raise NotImplementedError
