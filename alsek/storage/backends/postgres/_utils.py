"""

    Utils

"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod, ABC
from functools import cached_property
from typing import Any, AsyncIterable, Iterable, Optional

import asyncpg
import psycopg2
from sqlalchemy import URL
import logging

from alsek.storage.serialization import Serializer

log = logging.getLogger(__name__)


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


class PostgresPubSubListener(BasePostgresPubSubListen):
    def _get_connection(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(
            host=self.url.host,
            port=self.url.port,
            database=self.url.database,
            user=self.url.username,
            password=self.url.password,
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def _cleanup(
        self,
        conn: psycopg2.extensions.connection,
        cursor: Optional[psycopg2.extensions.cursor],
    ) -> None:
        try:
            if cursor:
                cursor.execute(f"UNLISTEN {self.channel}")
                cursor.close()
            conn.close()
        except Exception:  # noqa
            log.exception("Error on cleanup", exc_info=True)

    def listen(self) -> Iterable[Any]:
        conn = self._get_connection()
        cursor: Optional[psycopg2.extensions.cursor] = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"LISTEN {self.channel}")
            while True:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.popleft()  # type: ignore
                    if notify.channel == self.channel:
                        yield _parse_notification_data(
                            payload=notify.payload,
                            serializer=self.serializer,
                        )

                # Small sleep to prevent busy waiting
                time.sleep(self.sleep_time)
        finally:
            self._cleanup(conn, cursor=cursor)


class PostgresAsyncPubSubListener(BasePostgresPubSubListen):
    @cached_property
    def _notification_queue(self) -> asyncio.Queue[str]:
        return asyncio.Queue()

    async def _get_connection(self) -> asyncpg.Connection:
        return await asyncpg.connect(
            host=self.url.host,
            port=self.url.port,
            database=self.url.database,
            user=self.url.username,
            password=self.url.password,
        )

    def _notification_handler(
        self,
        connection: asyncpg.Connection,  # noqa
        pid: int,  # noqa
        channel: str,  # noqa
        payload: str,
    ) -> None:
        self._notification_queue.put_nowait(payload)

    async def _cleanup(self, conn: asyncpg.Connection) -> None:
        try:
            if not conn.is_closed():
                await conn.remove_listener(
                    self.channel,
                    callback=self._notification_handler,
                )
                await conn.close()
        except Exception:  # noqa
            log.exception("Error on cleanup", exc_info=True)

    async def listen(self) -> AsyncIterable[Any]:
        conn = await self._get_connection()
        await conn.add_listener(self.channel, callback=self._notification_handler)

        # Keep the connection alive and yield notifications
        try:
            while True:
                try:
                    notification = await asyncio.wait_for(
                        self._notification_queue.get(),
                        timeout=self.sleep_time,
                    )
                    yield _parse_notification_data(
                        payload=notification,
                        serializer=self.serializer,
                    )
                except asyncio.TimeoutError:
                    # No notification received within timeout, continue loop
                    continue
        finally:
            await self._cleanup(conn)
