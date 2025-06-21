"""

    Utils

"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod, ABC
from typing import Any, AsyncIterable, Iterable

import asyncpg
import psycopg2
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


class PostgresPubSubListener(BasePostgresPubSubListen):
    def _get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.url.host,
            port=self.url.port,
            database=self.url.database,
            user=self.url.username,
            password=self.url.password,
        )

    def _cleanup(
        self,
        conn: psycopg2.extensions.connection,
        cursor: psycopg2.extensions.cursor,
    ) -> None:
        try:
            cursor.execute(f"UNLISTEN {self.channel}")
            cursor.close()
            conn.close()
        except Exception:  # noqa
            pass

    def _stream(self) -> Iterable[Any]:
        conn = self._get_connection()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Start listening to the channel
        cursor.execute(f"LISTEN {self.channel}")
        try:
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

    def listen(self) -> Iterable[Any]:
        yield from self._stream()


class PostgresAsyncPubSubListener(BasePostgresPubSubListen):
    def __init__(
        self,
        channel: str,
        url: URL,
        serializer: Serializer,
        sleep_time: float = 0.01,
    ) -> None:
        super().__init__(
            channel=channel,
            url=url,
            serializer=serializer,
            sleep_time=sleep_time,
        )
        self._notification_queue: asyncio.Queue[str] = asyncio.Queue()

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
        try:
            self._notification_queue.put_nowait(payload)
        except asyncio.QueueFull:
            # Handle queue full scenario - could log warning
            pass

    async def _cleanup(self, conn: asyncpg.Connection) -> None:
        try:
            if not conn.is_closed():
                await conn.remove_listener(
                    self.channel,
                    callback=self._notification_handler,
                )
                await conn.close()
        except Exception:  # noqa
            pass

    async def _stream(self) -> AsyncIterable[Any]:
        conn = await self._get_connection()
        await conn.add_listener(self.channel, callback=self._notification_handler)

        # Keep the connection alive and yield notifications
        try:
            while True:
                try:
                    # Wait for notification with timeout to allow graceful shutdown
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

    async def listen(self) -> AsyncIterable[Any]:
        async for item in self._stream():
            yield item
