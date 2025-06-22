"""

    Standard Postgres Backend.

"""

from __future__ import annotations

from contextlib import contextmanager
from functools import cached_property
from typing import Optional, Union, cast, Iterator, Any, Type, Iterable

import dill
from sqlalchemy import Engine, create_engine, select, text, or_
from sqlalchemy.orm import Session
from sqlalchemy import URL

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient
from alsek.storage.backends.postgres.tables import (
    Base,
    KeyValue as KeyValueRecord,
    Priority as PriorityRecord,
    SCHEMA_NAME,
    KeyValueType,
)
from alsek.storage.backends.postgres._utils import (
    PostgresPubSubListener,
    validate_value_within_postgres_notification_size_limit,
)
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
            stmt = select(1).where(
                KeyValueRecord.id == self.full_name(name),
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )
            return session.scalars(stmt).first() is not None

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        full_name = self.full_name(name)
        with self.session() as session:
            obj = session.get(KeyValueRecord, full_name)

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
            session.commit()

    def get(
        self,
        name: str,
        default: Optional[Union[Any, Type[Empty]]] = None,
    ) -> Any:
        with self.session() as session:
            stmt = select(KeyValueRecord).where(
                KeyValueRecord.id == self.full_name(name),
                KeyValueRecord.type == KeyValueType.STANDARD,
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
        full_key = self.full_name(key)
        with self.session() as session:
            kv_obj = session.get(KeyValueRecord, full_key)
            if kv_obj is None:
                kv_obj = KeyValueRecord(
                    id=full_key,
                    value="",  # No value needed for priority queue metadata
                    type=KeyValueType.PRIORITY,
                )
                session.add(kv_obj)

            # Insert or update the Priority record
            if priority_obj := session.get(PriorityRecord, (full_key, unique_id)):
                priority_obj.priority = priority
            else:
                priority_obj = PriorityRecord(
                    id=full_key,
                    unique_id=unique_id,
                    priority=priority,
                )
                session.add(priority_obj)

            session.commit()

    def priority_get(self, key: str) -> Optional[str]:
        full_key = self.full_name(key)
        with self.session() as session:
            stmt = (
                select(PriorityRecord.unique_id)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
                .limit(1)
            )
            return session.scalars(stmt).first()

    def priority_iter(self, key: str) -> Iterable[str]:
        full_key = self.full_name(key)
        with self.session() as session:
            stmt = (
                select(PriorityRecord.unique_id)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
            )
            yield from session.scalars(stmt)

    def priority_remove(self, key: str, unique_id: str) -> None:
        full_key = self.full_name(key)
        with self.session() as session:
            # A. Remove the Priority record
            priority_obj = session.get(PriorityRecord, (full_key, unique_id))
            if priority_obj is None:
                return  # Nothing to remove

            session.delete(priority_obj)

            # B. Check if any Priority records remain for this queue
            remaining_priorities = session.scalars(
                select(1)
                .where(
                    PriorityRecord.id == full_key,
                    # Exclude what we're about to delete
                    PriorityRecord.unique_id != unique_id,
                )
                .limit(1)
            ).first()

            # C. If no priorities remain, clean up any KeyValue record
            if remaining_priorities is None and (kv_obj := session.get(KeyValueRecord, full_key)):  # fmt: skip
                session.delete(kv_obj)

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
            serialized_value = self.serializer.forward(value)

            validate_value_within_postgres_notification_size_limit(serialized_value)
            session.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": serialized_value},
            )
            session.commit()

    def sub(self, channel: str) -> Iterable[str | dict[str, Any]]:
        """Subscribe to a PostgreSQL channel using LISTEN.

        Args:
            channel (str): The channel name to subscribe to

        Returns:
            Iterable[str | dict[str, Any]]: An iterable of messages received on the channel

        """
        listener = PostgresPubSubListener(
            channel=channel,
            url=self.engine.url,
            serializer=self.serializer,
        )
        yield from listener.listen()

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        like_pattern = self.full_name((pattern or "%").replace("*", "%"))
        with self.session() as session:
            stmt = select(KeyValueRecord).where(
                KeyValueRecord.id.like(like_pattern),
                or_(
                    KeyValueRecord.expires_at.is_(None),
                    KeyValueRecord.expires_at > utcnow(),
                ),
            )

            for obj in session.scalars(stmt):
                if not obj.is_expired:
                    yield self.short_name(obj.id)

            session.commit()
