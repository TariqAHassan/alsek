"""

    Asynchronous Postgres Backend

"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional, Union, cast, AsyncIterator, Any, Type, AsyncIterable

import dill
from sqlalchemy import text, select, or_
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import AsyncBackend, LazyClient
from alsek.storage.backends.postgres.tables import (
    Base,
    KeyValue as KeyValueRecord,
    Priority as PriorityRecord,
    SCHEMA_NAME,
    KeyValueType,
)
from alsek.storage.backends.postgres._utils import PostgresAsyncPubSubListener
from alsek.storage.serialization import Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import compute_expiry_datetime, utcnow

log = logging.getLogger(__name__)


class PostgresAsyncBackend(AsyncBackend):
    """Asynchronous PostgreSQL backend powered by SQLAlchemy and asyncpg."""

    IS_ASYNC: bool = True
    SUPPORTS_PUBSUB: bool = True

    def __init__(
        self,
        engine: Union[str, AsyncEngine, LazyClient],
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Optional[Serializer] = None,
    ) -> None:
        super().__init__(namespace, serializer=serializer)

        self._engine = self._connection_parser(engine)
        self._tables_created: bool = False

    @staticmethod
    def _connection_parser(
        engine: Union[str, AsyncEngine, LazyClient],
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
    async def session(self) -> AsyncIterator[AsyncSession]:
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
        data["settings"]["engine"] = str(self.engine.url)
        return cast(bytes, dill.dumps(data))

    @classmethod
    def _from_settings(cls, settings: dict[str, Any]) -> PostgresAsyncBackend:
        settings["engine"] = create_async_engine(settings["engine"])
        return cls(**settings)

    async def exists(self, name: str) -> bool:
        async with self.session() as session:
            stmt = select(1).where(
                KeyValueRecord.id == self.full_name(name),
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )
            result = await session.execute(stmt)
            return result.scalars().first() is not None

    async def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        async with self.session() as session:
            full_name = self.full_name(name)
            obj = await session.get(KeyValueRecord, full_name)

            expires_at = compute_expiry_datetime(ttl)
            if nx and obj is not None:
                raise KeyError(f"Name '{name}' already exists")
            elif obj is None:
                obj = KeyValueRecord(
                    id=full_name,
                    value=self.serializer.forward(value),
                    type=KeyValueType.STANDARD,
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
        async with self.session() as session:
            stmt = select(KeyValueRecord).where(
                KeyValueRecord.id == self.full_name(name),
                KeyValueRecord.type == KeyValueType.STANDARD,
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )
            obj: Optional[KeyValueRecord] = (await session.scalars(stmt)).one_or_none()
            if obj is None or obj.is_expired:
                if default is Empty or isinstance(default, Empty):
                    raise KeyError(f"No name '{name}' found")
                return default
            return self.serializer.reverse(obj.value)

    async def delete(self, name: str, missing_ok: bool = False) -> None:
        async with self.session() as session:
            obj = await session.get(KeyValueRecord, self.full_name(name))
            if obj is None:
                if not missing_ok:
                    raise KeyError(f"No name '{name}' found")
                return
            await session.delete(obj)
            await session.commit()

    async def priority_add(
        self,
        key: str,
        unique_id: str,
        priority: int | float,
    ) -> None:
        full_key = self.full_name(key)
        async with self.session() as session:
            # Ensure the KeyValue record exists for the priority queue
            kv_obj = await session.get(KeyValueRecord, full_key)
            if kv_obj is None:
                kv_obj = KeyValueRecord(
                    id=full_key,
                    value="",  # No value needed for priority queue metadata
                    type=KeyValueType.PRIORITY,
                )
                session.add(kv_obj)

            # Insert or update the Priority record
            if priority_obj := await session.get(PriorityRecord, (full_key, unique_id)):
                priority_obj.priority = priority
            else:
                priority_obj = PriorityRecord(
                    id=full_key,
                    unique_id=unique_id,
                    priority=priority,
                )
                session.add(priority_obj)

            await session.commit()

    async def priority_get(self, key: str) -> Optional[str]:
        full_key = self.full_name(key)
        async with self.session() as session:
            stmt = (
                select(PriorityRecord.unique_id)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalars().first()

    async def priority_iter(self, key: str) -> AsyncIterable[str]:
        full_key = self.full_name(key)
        async with self.session() as session:
            stmt = (
                select(PriorityRecord.unique_id)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
            )
            result = await session.execute(stmt)
            for unique_id in result.scalars():
                yield unique_id

    async def priority_remove(self, key: str, unique_id: str) -> None:
        full_key = self.full_name(key)
        async with self.session() as session:
            # A. Remove the Priority record
            priority_obj = await session.get(PriorityRecord, (full_key, unique_id))
            if priority_obj is None:
                return  # Nothing to remove

            await session.delete(priority_obj)

            # B. Check if any Priority records remain for this queue
            remaining_priorities = (
                (
                    await session.execute(
                        select(1)
                        .where(
                            PriorityRecord.id == full_key,
                            # Exclude what we're about to delete
                            PriorityRecord.unique_id != unique_id,
                        )
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )

            # C. If no priorities remain, clean up any KeyValue record
            if remaining_priorities is None and (kv_obj := await session.get(KeyValueRecord, full_key)):  # fmt: skip
                await session.delete(kv_obj)

            await session.commit()

    async def pub(self, channel: str, value: Any) -> None:
        """Publish a message to a PostgreSQL channel using NOTIFY.

        Args:
            channel (str): The channel name to publish to
            value (Any): The value to publish (will be serialized)

        Returns:
            None

        """
        async with self.session() as session:
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
        listener = PostgresAsyncPubSubListener(
            channel=channel,
            url=self.engine.url,
            serializer=self.serializer,
        )
        async for message in listener.listen():
            yield message

    async def scan(self, pattern: Optional[str] = None) -> AsyncIterable[str]:
        like_pattern = self.full_name((pattern or "%").replace("*", "%"))
        async with self.session() as session:
            stmt = select(KeyValueRecord).where(
                KeyValueRecord.id.like(like_pattern),
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )

            for obj in (await session.execute(stmt)).scalars():
                if not obj.is_expired:
                    yield self.short_name(obj.id)
