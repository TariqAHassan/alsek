"""

    Asynchronous Postgres Backend

"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional, Union, cast, AsyncIterator, Any, Type, AsyncIterable

import dill
from sqlalchemy import text, select, delete
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import AsyncBackend, LazyClient
from alsek.storage.backends.postgres._tables import (
    Base,
    KeyValue as KeyValueRecord,
    Priority as PriorityRecord,
    SCHEMA_NAME,
)
from alsek.storage.backends.postgres._utils import PostgresAsyncPubSubListener
from alsek.storage.serialization import Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import utcnow_timestamp_ms

log = logging.getLogger(__name__)


class PostgresAsyncBackend(AsyncBackend):
    """Asynchronous PostgreSQL backend powered by SQLAlchemy and asyncpg."""

    IS_ASYNC: bool = True
    SUPPORTS_PUBSUB: bool = True

    def __init__(
        self,
        engine: Union[str, AsyncEngine, LazyClient] = None,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Optional[Serializer] = None,
    ) -> None:
        super().__init__(namespace, serializer=serializer)

        self._engine = self._engine_parse(engine)
        self._tables_created: bool = False

    @staticmethod
    def _engine_parse(
        engine: Optional[Union[str, AsyncEngine, LazyClient]],
    ) -> Union[AsyncEngine, LazyClient]:
        if isinstance(engine, LazyClient):
            return engine
        elif isinstance(engine, AsyncEngine):
            return engine
        elif isinstance(engine, str):
            # Convert sync URL to async URL if needed
            if "://" in engine and "+asyncpg" not in engine:
                engine = engine.replace("postgresql://", "postgresql+asyncpg://")
            return create_async_engine(engine)
        else:
            raise ValueError(f"Unsupported `engine` {engine}")

    @property
    def engine(self) -> AsyncEngine:
        if isinstance(self._engine, LazyClient):
            self._engine = self._engine.get()
        return cast(AsyncEngine, self._engine)

    async def _ensure_schema_and_tables_exist(self) -> None:
        if not self._tables_created:
            async with self.engine.begin() as conn:
                await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
                await conn.run_sync(Base.metadata.create_all)
            self._tables_created = True

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[AsyncSession]:
        await self._ensure_schema_and_tables_exist()
        async with AsyncSession(self.engine) as session:
            yield session

    def __repr__(self) -> str:
        return auto_repr(
            self,
            namespace=self.namespace,
            serializer=self.serializer,
        )

    def encode(self) -> bytes:
        data: dict[str, Any] = dict(
            backend=self.__class__,
            settings=gather_init_params(self, ignore=("engine",)),
        )
        # For async engines, we need to handle the URL differently
        data["settings"]["engine"] = str(self.engine.url)
        return cast(bytes, dill.dumps(data))

    @classmethod
    def _from_settings(cls, settings: dict[str, Any]) -> PostgresAsyncBackend:
        settings["engine"] = create_async_engine(settings["engine"])
        return cls(**settings)

    @staticmethod
    def _cleanup_expired(session: AsyncSession, obj: KeyValueRecord) -> bool:
        if obj.expires_at is not None and obj.expires_at <= utcnow_timestamp_ms():
            session.delete(obj)
            return True
        return False

    async def exists(self, name: str) -> bool:
        async with self._session() as session:
            obj: Optional[KeyValueRecord] = await session.get(
                KeyValueRecord, self.full_name(name)
            )
            if obj is None:
                return False
            expired = self._cleanup_expired(session, obj)
            if expired:
                await session.commit()
            return not expired

    async def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        async with self._session() as session:
            full_name = self.full_name(name)
            expires_at = utcnow_timestamp_ms() + ttl if ttl is not None else None
            obj = await session.get(KeyValueRecord, full_name)
            if nx and obj is not None:
                raise KeyError(f"Name '{name}' already exists")
            if obj is None:
                obj = KeyValueRecord(
                    name=full_name,
                    value=self.serializer.forward(value),
                    expires_at=expires_at,
                )
                session.add(obj)
            else:
                obj.value = self.serializer.forward(value)
                obj.expires_at = expires_at
            await session.commit()

    async def get(
        self,
        name: str,
        default: Optional[Union[Any, Type[Empty]]] = Empty,
    ) -> Any:
        async with self._session() as session:
            obj: Optional[KeyValueRecord] = await session.get(
                KeyValueRecord, self.full_name(name)
            )
            if obj is None or self._cleanup_expired(session, obj):
                if obj is not None:  # cleanup was performed
                    await session.commit()
                if default is Empty or isinstance(default, Empty):
                    raise KeyError(f"No name '{name}' found")
                return default
            value = self.serializer.reverse(obj.value)
            return value

    async def delete(self, name: str, missing_ok: bool = False) -> None:
        async with self._session() as session:
            obj = await session.get(KeyValueRecord, self.full_name(name))
            if obj is None:
                if not missing_ok:
                    raise KeyError(f"No name '{name}' found")
                return
            await session.delete(obj)
            await session.commit()

    async def priority_add(
        self, key: str, unique_id: str, priority: int | float
    ) -> None:
        async with self._session() as session:
            full_key = self.full_name(key)
            stmt = select(PriorityRecord).where(
                PriorityRecord.key == full_key,
                PriorityRecord.unique_id == unique_id,
            )
            result = await session.execute(stmt)
            obj = result.scalars().first()
            if obj is None:
                obj = PriorityRecord(
                    key=full_key,
                    unique_id=unique_id,
                    priority=priority,
                )
                session.add(obj)
            else:
                obj.priority = priority
            await session.commit()

    async def priority_get(self, key: str) -> Optional[str]:
        async with self._session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(PriorityRecord)
                .where(PriorityRecord.key == full_key)
                .order_by(PriorityRecord.priority.asc())
                .limit(1)
            )
            result = await session.execute(stmt)
            obj = result.scalars().first()
            return obj.unique_id if obj else None

    async def priority_iter(self, key: str) -> AsyncIterable[str]:
        async with self._session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(PriorityRecord)
                .where(PriorityRecord.key == full_key)
                .order_by(PriorityRecord.priority.asc())
            )
            result = await session.execute(stmt)
            for obj in result.scalars():
                yield obj.unique_id

    async def priority_remove(self, key: str, unique_id: str) -> None:
        async with self._session() as session:
            full_key = self.full_name(key)
            stmt = delete(PriorityRecord).where(
                PriorityRecord.key == full_key,
                PriorityRecord.unique_id == unique_id,
            )
            await session.execute(stmt)
            await session.commit()

    async def pub(self, channel: str, value: Any) -> None:
        """Publish a message to a PostgreSQL channel using NOTIFY.

        Args:
            channel (str): The channel name to publish to
            value (Any): The value to publish (will be serialized)

        Returns:
            None

        """
        async with self._session() as session:
            # Serialize the value
            serialized_value = self.serializer.forward(value)

            # Use NOTIFY to publish the message
            # PostgreSQL NOTIFY has a payload limit of 8000 bytes
            if len(serialized_value) > 8000:
                raise ValueError(
                    "Message payload too large for PostgreSQL NOTIFY (max 8000 bytes)"
                )

            stmt = text("NOTIFY :channel, :payload")
            await session.execute(
                stmt, {"channel": channel, "payload": serialized_value}
            )
            await session.commit()

    async def sub(self, channel: str) -> AsyncIterable[str | dict[str, Any]]:
        """Subscribe to a PostgreSQL channel using LISTEN.

        Args:
            channel (str): The channel name to subscribe to

        Returns:
            AsyncIterable[str | dict[str, Any]]: An async iterable of messages received on the channel

        """
        # Create an async listener using the engine URL
        engine = self.engine
        listener = PostgresAsyncPubSubListener(
            channel=channel,
            url=engine.url,
            serializer=self.serializer,
        )
        async for message in listener.listen():
            yield message

    async def scan(self, pattern: Optional[str] = None) -> AsyncIterable[str]:
        async with self._session() as session:
            like_pattern = self.full_name(pattern or "%").replace("*", "%")
            stmt = select(KeyValueRecord)
            if like_pattern:
                stmt = stmt.where(KeyValueRecord.name.like(like_pattern))

            result = await session.execute(stmt)
            expired_objects = []

            for obj in result.scalars():
                if (
                    obj.expires_at is not None
                    and obj.expires_at <= utcnow_timestamp_ms()
                ):
                    expired_objects.append(obj)
                else:
                    yield self.short_name(obj.name)

            # Clean up expired objects
            for obj in expired_objects:
                await session.delete(obj)
            if expired_objects:
                await session.commit()
