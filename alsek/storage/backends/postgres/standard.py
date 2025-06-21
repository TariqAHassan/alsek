"""

    Standard Postgres Backend.

"""

from __future__ import annotations

from contextlib import contextmanager
from functools import cached_property
from typing import Optional, Union, cast, Iterator, Any, Type, Iterable

import dill
from sqlalchemy import Engine, create_engine, select, delete, text, or_
from sqlalchemy.orm import Session
from sqlalchemy import URL

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient
from alsek.storage.backends.postgres.tables import (
    Base,
    KeyValue as KeyValueRecord,
    Priority as PriorityRecord,
    SCHEMA_NAME,
)
from alsek.storage.backends.postgres._utils import PostgresPubSubListener
from alsek.storage.serialization import Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import compute_expiry_datetime, utcnow


class PostgresBackend(Backend):
    """PostgreSQL backend powered by SQLAlchemy."""

    SUPPORTS_PUBSUB: bool = True

    def __init__(
        self,
        engine: Union[str, URL, Engine, LazyClient],
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Optional[Serializer] = None,
    ) -> None:
        super().__init__(namespace, serializer=serializer)

        self._engine = self._connection_parser(engine)
        self._tables_created: bool = False

    @staticmethod
    def _connection_parser(
        engine: Union[str, URL, Engine, LazyClient]
    ) -> Union[Engine, LazyClient]:
        if isinstance(engine, LazyClient):
            return engine
        elif isinstance(engine, Engine):
            return engine
        elif isinstance(engine, (str, URL)):
            return create_engine(engine)
        else:
            raise ValueError(f"Unsupported `engine` {engine}")

    @cached_property
    def engine(self) -> Engine:
        if isinstance(self._engine, LazyClient):
            self._engine = self._engine.get()
        return cast(Engine, self._engine)

    def _ensure_schema_and_tables_exist(self) -> None:
        if not self._tables_created:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
                conn.commit()
            Base.metadata.create_all(self.engine)
            self._tables_created = True

    @contextmanager
    def session(self) -> Iterator[Session]:
        self._ensure_schema_and_tables_exist()
        with Session(self.engine) as session:
            yield session

    def __repr__(self) -> str:
        return auto_repr(
            self,
            engine=self.engine,
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
    def _from_settings(cls, settings: dict[str, Any]) -> PostgresBackend:
        settings["engine"] = create_engine(settings["engine"])
        return cls(**settings)

    def exists(self, name: str) -> bool:
        with self.session() as session:
            obj: Optional[KeyValueRecord] = session.get(
                KeyValueRecord,
                self.full_name(name),
            )
            if obj is None:
                return False
            else:
                return not obj.is_expired

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        with self.session() as session:
            full_name = self.full_name(name)
            obj = session.get(KeyValueRecord, full_name)

            expires_at = compute_expiry_datetime(ttl)
            if nx and obj is not None:
                raise KeyError(f"Name '{name}' already exists")
            elif obj is None:
                obj = KeyValueRecord(
                    id=full_name,
                    value=self.serializer.forward(value),
                    expires_at=expires_at,
                )
                session.add(obj)
            else:
                obj.value = self.serializer.forward(value)
                obj.expires_at = expires_at
            session.commit()

    def get(
        self,
        name: str,
        default: Optional[Union[Any, Type[Empty]]] = None,
    ) -> Any:
        with self.session() as session:
            stmt = select(KeyValueRecord).where(
                KeyValueRecord.id == self.full_name(name),
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )
            obj: Optional[KeyValueRecord] = session.scalars(stmt).one_or_none()
            if obj is None or obj.is_expired:
                if default is Empty or isinstance(default, Empty):
                    raise KeyError(f"No name '{name}' found")
                return default
            return self.serializer.reverse(obj.value)

    def delete(self, name: str, missing_ok: bool = False) -> None:
        with self.session() as session:
            obj = session.get(KeyValueRecord, self.full_name(name))
            if obj is None:
                if not missing_ok:
                    raise KeyError(f"No name '{name}' found")
                return
            session.delete(obj)
            session.commit()

    def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
        with self.session() as session:
            full_key = self.full_name(key)
            stmt = select(PriorityRecord).where(
                PriorityRecord.id == full_key,
                PriorityRecord.unique_id == unique_id,
            )
            obj = session.scalars(stmt).one_or_none()
            if obj is None:
                obj = PriorityRecord(
                    id=full_key,
                    unique_id=unique_id,
                    priority=priority,
                )
                session.add(obj)
            else:
                obj.priority = priority
            session.commit()

    def priority_get(self, key: str) -> Optional[str]:
        with self.session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(PriorityRecord)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
                .limit(1)
            )
            obj = session.scalars(stmt).one_or_none()
            return obj.unique_id if obj else None

    def priority_iter(self, key: str) -> Iterable[str]:
        with self.session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(PriorityRecord)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
            )
            for obj in session.scalars(stmt):
                yield obj.unique_id

    def priority_remove(self, key: str, unique_id: str) -> None:
        with self.session() as session:
            full_key = self.full_name(key)
            stmt = delete(PriorityRecord).where(
                PriorityRecord.id == full_key,
                PriorityRecord.unique_id == unique_id,
            )
            session.execute(stmt)
            session.commit()

    def pub(self, channel: str, value: Any) -> None:
        """Publish a message to a PostgreSQL channel using NOTIFY.

        Args:
            channel (str): The channel name to publish to
            value (Any): The value to publish (will be serialized)

        Returns:
            None

        """
        with self.session() as session:
            # Serialize the value
            serialized_value = self.serializer.forward(value)

            # Use NOTIFY to publish the message
            # PostgreSQL NOTIFY has a payload limit of 8000 bytes
            if len(serialized_value) > 8000:
                raise ValueError(
                    "Message payload too large for PostgreSQL NOTIFY (max 8000 bytes)"
                )

            stmt = text("NOTIFY :channel, :payload")
            session.execute(stmt, {"channel": channel, "payload": serialized_value})
            session.commit()

    def sub(self, channel: str) -> Iterable[str | dict[str, Any]]:
        """Subscribe to a PostgreSQL channel using LISTEN.

        Args:
            channel (str): The channel name to subscribe to

        Returns:
            Iterable[str | dict[str, Any]]: An iterable of messages received on the channel

        """
        # Create a direct psycopg2 connection from the engine URL
        listener = PostgresPubSubListener(
            channel=channel,
            url=self.engine.url,
            serializer=self.serializer,
        )
        yield from listener.listen()

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        with self.session() as session:
            like_pattern = self.full_name(pattern or "%").replace("*", "%")
            stmt = select(KeyValueRecord)
            if like_pattern:
                stmt = stmt.where(KeyValueRecord.id.like(like_pattern))

            for obj in session.scalars(stmt):
                if not obj.is_expired:
                    yield self.short_name(obj.id)
            session.commit()
