"""

    Standard PubSub Listener

"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable, Optional

import psycopg2
from psycopg2 import sql

from alsek.storage.backends.postgres.utils.pubsub import BasePostgresPubSubListen

log = logging.getLogger(__name__)


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
                cursor.execute(
                    sql.SQL("UNLISTEN {}").format(sql.Identifier(self.channel))
                )
                cursor.close()
            conn.close()
        except Exception:  # noqa
            log.exception("Error on cleanup", exc_info=True)

    def listen(self) -> Iterable[Any]:
        conn = self._get_connection()
        cursor: Optional[psycopg2.extensions.cursor] = None
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)),
            )
            while True:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    if notify.channel == self.channel:
                        yield self._parse_notification_data(
                            payload=notify.payload,
                            serializer=self.serializer,
                        )

                # Small sleep to prevent busy waiting
                time.sleep(self.sleep_time)
        finally:
            self._cleanup(conn, cursor=cursor)
