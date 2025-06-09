"""
Postgres Backend
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from typing import Any, Iterable, Iterator, Optional, Type, Union, cast

import dill
from sqlalchemy import Column, Float, String, create_engine, delete, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient
from alsek.storage.serialization import JsonSerializer, Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import utcnow_timestamp_ms

log = logging.getLogger(__name__)

Base = declarative_base()


class KV(Base):
    __tablename__ = "alsek_kv"

    name = Column(String, primary_key=True)
    value = Column(String)
    expires_at = Column(Float, nullable=True)


class Priority(Base):
    __tablename__ = "alsek_priority"

    key = Column(String, primary_key=True)
    unique_id = Column(String, primary_key=True)
    priority = Column(Float)


class PostgresBackend(Backend):
    """PostgreSQL backend powered by SQLAlchemy."""

    SUPPORTS_PUBSUB: bool = False

    def __init__(
        self,
        engine: Optional[Union[str, Engine, LazyClient]] = None,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Serializer = JsonSerializer(),
    ) -> None:
        super().__init__(namespace, serializer=serializer)
        self._engine = self._engine_parse(engine)
        if not isinstance(self._engine, LazyClient):
            Base.metadata.create_all(self.engine)

    @staticmethod
    def _engine_parse(
        engine: Optional[Union[str, Engine, LazyClient]],
    ) -> Union[Engine, LazyClient]:
        if isinstance(engine, LazyClient):
            return engine
        if engine is None:
            return create_engine("postgresql://localhost/postgres")
        elif isinstance(engine, Engine):
            return engine
        elif isinstance(engine, str):
            return create_engine(engine)
        else:
            raise ValueError(f"Unsupported `engine` {engine}")

    @cached_property
    def engine(self) -> Engine:
        if isinstance(self._engine, LazyClient):
            self._engine = self._engine.get()
            Base.metadata.create_all(cast(Engine, self._engine))
        return cast(Engine, self._engine)

    @contextmanager
    def _session(self) -> Iterator[Session]:
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
    def _from_settings(cls, settings: dict[str, Any]) -> "PostgresBackend":
        settings["engine"] = create_engine(settings["engine"])
        return cls(**settings)

    def _cleanup_expired(self, session: Session, obj: KV) -> bool:
        if obj.expires_at is not None and obj.expires_at <= utcnow_timestamp_ms():
            session.delete(obj)
            session.commit()
            return True
        return False

    def exists(self, name: str) -> bool:
        with self._session() as session:
            obj = session.get(KV, self.full_name(name))
            if obj is None:
                return False
            expired = self._cleanup_expired(session, obj)
            return not expired

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        with self._session() as session:
            full_name = self.full_name(name)
            expires_at = utcnow_timestamp_ms() + ttl if ttl is not None else None
            obj = session.get(KV, full_name)
            if nx and obj is not None:
                raise KeyError(f"Name '{name}' already exists")
            if obj is None:
                obj = KV(
                    name=full_name,
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
        with self._session() as session:
            obj = session.get(KV, self.full_name(name))
            if obj is None or self._cleanup_expired(session, obj):
                if default is Empty or isinstance(default, Empty):
                    raise KeyError(f"No name '{name}' found")
                return default
            value = self.serializer.reverse(obj.value)
            return value

    def delete(self, name: str, missing_ok: bool = False) -> None:
        with self._session() as session:
            obj = session.get(KV, self.full_name(name))
            if obj is None:
                if not missing_ok:
                    raise KeyError(f"No name '{name}' found")
                return
            session.delete(obj)
            session.commit()

    def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
        with self._session() as session:
            full_key = self.full_name(key)
            stmt = select(Priority).where(
                Priority.key == full_key, Priority.unique_id == unique_id
            )
            obj = session.scalars(stmt).first()
            if obj is None:
                obj = Priority(key=full_key, unique_id=unique_id, priority=priority)
                session.add(obj)
            else:
                obj.priority = priority
            session.commit()

    def priority_get(self, key: str) -> Optional[str]:
        with self._session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(Priority)
                .where(Priority.key == full_key)
                .order_by(Priority.priority.asc())
                .limit(1)
            )
            obj = session.scalars(stmt).first()
            return obj.unique_id if obj else None

    def priority_iter(self, key: str) -> Iterable[str]:
        with self._session() as session:
            full_key = self.full_name(key)
            stmt = (
                select(Priority)
                .where(Priority.key == full_key)
                .order_by(Priority.priority.asc())
            )
            for obj in session.scalars(stmt):
                yield obj.unique_id

    def priority_remove(self, key: str, unique_id: str) -> None:
        with self._session() as session:
            full_key = self.full_name(key)
            stmt = delete(Priority).where(
                Priority.key == full_key, Priority.unique_id == unique_id
            )
            session.execute(stmt)
            session.commit()

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        with self._session() as session:
            like_pattern = self.full_name(pattern or "%").replace("*", "%")
            stmt = select(KV)
            if like_pattern:
                stmt = stmt.where(KV.name.like(like_pattern))
            now = utcnow_timestamp_ms()
            for obj in session.scalars(stmt):
                if obj.expires_at is not None and obj.expires_at <= now:
                    session.delete(obj)
                else:
                    yield self.short_name(obj.name)
            session.commit()
