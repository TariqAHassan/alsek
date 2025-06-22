from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Any, AsyncIterable

from sqlalchemy import URL

from alsek.storage.serialization import Serializer


def _parse_notification_data(payload: str, serializer: Serializer) -> dict[str, Any]:
    data = serializer.reverse(payload)
    return {"type": "message", "data": data}


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

    @abstractmethod
    def listen(self) -> Iterable[Any] | AsyncIterable[Any]:
        raise NotImplementedError
