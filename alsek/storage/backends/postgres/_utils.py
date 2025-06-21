from __future__ import annotations

import time
from functools import cached_property

from typing import Any, Iterable

import psycopg2
from sqlalchemy import URL

from alsek.storage.serialization import Serializer


def _parse_notification_data(payload: str, serializer: Serializer) -> dict[str, Any]:
    data = serializer.reverse(payload)
    return {"type": "message", "data": data}


class PostgresPubSubListener:
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

        self.cursor = None

    @cached_property
    def conn(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.url.host,
            port=self.url.port,
            database=self.url.database,
            user=self.url.username,
            password=self.url.password,
        )

    def _listen(self) -> Iterable[Any]:
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = self.conn.cursor()

        # Start listening to the channel
        cursor.execute(f"LISTEN {self.channel}")
        while True:
            self.conn.poll()
            while self.conn.notifies:
                notify = conn.notifies.popleft()  # type: ignore
                if notify.channel == self.channel:
                    yield _parse_notification_data(
                        payload=notify.payload,
                        serializer=self.serializer,
                    )

            # Small sleep to prevent busy waiting
            time.sleep(self.sleep_time)

    def _cleanup(self) -> None:
        try:
            if self.cursor:
                self.cursor.execute(f"UNLISTEN {self.channel}")
                self.cursor.close()
        except Exception:  # noqa
            pass
        self.conn.close()

    def run(self) -> Iterable[Any]:
        try:
            yield from self._listen()
        finally:
            self._cleanup()
