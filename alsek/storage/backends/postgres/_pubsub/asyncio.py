from __future__ import annotations

import asyncio
from functools import cached_property
from typing import AsyncIterable, Any

import asyncpg

from alsek.storage.backends.postgres._pubsub.base import (
    BasePostgresPubSubListen,
    _parse_notification_data,
)

import logging

log = logging.getLogger(__name__)


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
