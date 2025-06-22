"""

    Standard Postgres Backend.

"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from functools import cached_property
from typing import Any, Iterable, Iterator, Optional, Type, Union, cast

import dill
from sqlalchemy import URL, Engine, create_engine, or_, select, text
from sqlalchemy.orm import Session

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient
from alsek.storage.backends.postgres._utils import (
    validate_value_within_postgres_notification_size_limit,
)
from alsek.storage.backends.postgres.standard._pubsub_listener import PostgresPubSubListener
from alsek.storage.backends.postgres.tables import SCHEMA_NAME, Base
from alsek.storage.backends.postgres.tables import KeyValue as KeyValueRecord
from alsek.storage.backends.postgres.tables import KeyValueType
from alsek.storage.backends.postgres.tables import Priority as PriorityRecord
from alsek.storage.serialization import Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import compute_expiry_datetime, utcnow

# Global lock to prevent race conditions when multiple backend instances
# try to create schema/tables on the same database simultaneously
_TABLES_CREATION_LOCK = threading.Lock()


class PostgresBackend(Backend):
    """PostgreSQL backend powered by SQLAlchemy.

    Backend powered by PostgreSQL using SQLAlchemy for ORM operations.
    Provides full support for key-value storage, priority queues, and pub/sub messaging.

    Args:
        engine (Union[str, URL, Engine, LazyClient]): A connection URL string,
            SQLAlchemy URL object, SQLAlchemy Engine instance, or LazyClient.
        namespace (str): Prefix to use when inserting names in the backend.
        serializer (Serializer, optional): Tool for encoding and decoding
            values written into the backend.

    """

    __SUPPORTS_PUBSUB__: bool = True

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
        # Double-checked locking pattern for thread safety
        if not self._tables_created:
            with _TABLES_CREATION_LOCK:
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
        """Check if a key exists in the PostgreSQL backend.

        Args:
            name (str): The name of the key to check.

        Returns:
            bool: `True` if the key exists and is not expired, `False` otherwise.

        """
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
        """Set a value for a key in the PostgreSQL backend.

        Args:
            name (str): The name of the key.
            value (Any): The value to set.
            nx (bool, optional): If `True`, only set the key if it does not already exist.
            ttl (Optional[int], optional): Time to live for the key in milliseconds.

        Returns:
            None

        Raises:
            KeyError: If `nx` is `True` and the key already exists.

        """
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
        """Get the value of a key from the PostgreSQL backend.

        Args:
            name (str): The name of the key.
            default (Optional[Union[Any, Type[Empty]]], optional): Default value if the key does not exist.

        Returns:
            Any: The value of the key.

        Raises:
            KeyError: If the key does not exist and no default is provided.

        """
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
        """Delete a key from the PostgreSQL backend.

        Args:
            name (str): The name of the key to delete.
            missing_ok (bool, optional): If `True`, do not raise an error if the key does not exist.

        Returns:
            None

        Raises:
            KeyError: If the key does not exist and `missing_ok` is `False`.

        """
        with self.session() as session:
            obj = session.get(KeyValueRecord, self.full_name(name))
            if obj is None:
                if not missing_ok:
                    raise KeyError(f"No name '{name}' found")
                return
            session.delete(obj)
            session.commit()

    def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
        """Add an item to a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The item's unique identifier.
            priority (int | float): The numeric priority score.

        Returns:
            None

        """
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
        """Peek the highest-priority item in a sorted set.

        Args:
            key (str): The name of the sorted set.

        Returns:
            Optional[str]: The ID of the highest-priority item, or None if empty.

        """
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
        """Iterate over all items in a priority-sorted set.

        Args:
            key (str): The name of the sorted set.

        Yields:
            str: Member of the sorted set, in priority order.

        """
        full_key = self.full_name(key)
        with self.session() as session:
            stmt = (
                select(PriorityRecord.unique_id)
                .where(PriorityRecord.id == full_key)
                .order_by(PriorityRecord.priority.asc())
            )
            yield from session.scalars(stmt)

    def priority_remove(self, key: str, unique_id: str) -> None:
        """Remove an item from a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The ID of the item to remove.

        Returns:
            None

        """
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
            channel (str): The name of the channel.
            value (Any): The message to publish.

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
            channel (str): The name of the channel to subscribe to.

        Returns:
            Iterable[str | dict[str, Any]]: An iterable of messages received on the channel.

        """
        listener = PostgresPubSubListener(
            channel=channel,
            url=self.engine.url,
            serializer=self.serializer,
        )
        yield from listener.listen()

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        """Scan the PostgreSQL backend for keys matching a pattern.

        Args:
            pattern (Optional[str], optional): The pattern to match keys against. Defaults to '*'.

        Yields:
            str: The names of matching keys without the namespace prefix.

        """
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
