"""

    Redis Backend

"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Type, Union, cast

import dill
from redis import ConnectionPool, Redis

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.storage.backends.abstract import Backend
from alsek.storage.backends.lazy import LazyClient
from alsek.storage.serialization import JsonSerializer, Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr


def parse_sub_data(data: dict[str, Any], serializer: Serializer) -> dict[str, Any]:
    if data.get("type", "").lower() == "message" and data.get("data") is not None:
        data["data"] = serializer.reverse(data["data"])
    return data


class RedisBackend(Backend):
    """Redis Backend.

    Backend powered by Redis.

    Args:
        conn (str, Redis, LazyClient, optional): a connection url, ``Redis()`` object
            or ``LazyClient``.
        namespace (str): prefix to use when inserting
            names in the backend
        serializer (Serializer): tool for encoding and decoding
            values written into the backend.

    Warning:
        * If ``conn`` is a ``Redis()`` object, ``decode_responses``
          is expected to be set to ``True``.

    """

    SUPPORTS_PUBSUB: bool = True

    def __init__(
        self,
        conn: Optional[Union[str, Redis, LazyClient]] = None,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Serializer = JsonSerializer(),
    ) -> None:
        super().__init__(namespace, serializer=serializer)
        self._conn = self._conn_parse(conn)

    @staticmethod
    def _conn_parse(
        conn: Optional[Union[str, Redis, LazyClient]]
    ) -> Union[Redis, LazyClient]:
        if isinstance(conn, LazyClient):
            return conn

        if conn is None:
            return Redis(decode_responses=True)
        elif isinstance(conn, Redis):
            return conn
        elif isinstance(conn, str):
            return Redis.from_url(conn, decode_responses=True)
        else:
            raise ValueError(f"Unsupported `conn` {conn}")

    @property
    def conn(self) -> Redis:
        """Connection to the backend."""
        if isinstance(self._conn, LazyClient):
            self._conn = self._conn.get()
        return cast(Redis, self._conn)

    def __repr__(self) -> str:
        return auto_repr(
            self,
            conn=self.conn,
            namespace=self.namespace,
            serializer=self.serializer,
        )

    def encode(self) -> bytes:
        data: dict[str, Any] = dict(
            backend=self.__class__,
            settings=gather_init_params(self, ignore=("conn",)),
        )
        data["settings"]["conn"] = dict(
            connection_class=self.conn.connection_pool.connection_class,
            max_connections=self.conn.connection_pool.max_connections,
            connection_kwargs=self.conn.connection_pool.connection_kwargs,
        )
        return cast(bytes, dill.dumps(data))

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> RedisBackend:
        settings["conn"] = Redis(
            connection_pool=ConnectionPool(
                connection_class=settings["conn"]["connection_class"],
                max_connections=settings["conn"]["max_connections"],
                **settings["conn"]["connection_kwargs"],
            )
        )
        return cls(**settings)

    def exists(self, name: str) -> bool:
        """Check if ``name`` exists in the Redis backend.

        Args:
            name (str): name of the item

        Returns:
            bool

        """
        return bool(self.conn.exists(self.full_name(name)))

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        """Set ``name`` to ``value`` in the Redis backend.

        Args:
            name (str): name of the item
            value (Any): value to set for ``name``
            nx (bool): if ``True`` the item must not exist prior to being set
            ttl (int, optional): time to live for the entry in milliseconds

        Returns:
            None

        Raises:
            KeyError: if ``nx`` is ``True`` and ``name`` already exists

        """
        response = self.conn.set(
            self.full_name(name),
            value=self.serializer.forward(value),
            px=ttl,
            nx=nx,
            keepttl=ttl is None,  # type: ignore
        )
        if nx and response is None:
            raise KeyError(f"Name '{name}' already exists")

    def get(self, name: str, default: Optional[Union[Any, Type[Empty]]] = None) -> Any:
        """Get ``name`` from the Redis backend.

        Args:
            name (str): name of the item
            default (Any, Type[Empty], optional): default value for ``name``

        Returns:
            Any

        """
        return self._get_engine(
            lambda: self.conn.__getitem__(self.full_name(name)),
            default=default,
        )

    def delete(self, name: str, missing_ok: bool = False) -> None:
        """Delete a ``name`` from the Redis backend.

        Args:
            name (str): name of the item
            missing_ok (bool): if ``True``, do not raise for missing

        Returns:
            None

        Raises:
            KeyError: if ``missing_ok`` is ``False`` and ``name`` is not found.

        """
        found = self.conn.delete(self.full_name(name))
        if not missing_ok and not found:
            raise KeyError(f"No name '{name}' found")

    def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
        """Add an item to a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The item's (Message's) unique identifier
            priority (float): The numeric priority score (decide if lower or higher means higher priority).

        Returns:
            None

        """
        self.conn.zadd(
            self.full_name(key),
            mapping={unique_id: priority},
        )

    def priority_get(self, key: str) -> Optional[str]:
        """Get (peek) the highest-priority item without removing it.

        Args:
            key (str): The name of the sorted set.

        Returns:
            item (str, optional): The member with the highest priority, or None if empty.

        """
        results: list[str] = self.conn.zrange(
            self.full_name(key),
            start=0,
            end=0,
        )
        return results[0] if results else None

    def priority_iter(self, key: str) -> Iterable[str]:
        """Iterate over the items in a priority-sorted set.

        Args:
            key (str): The name of the sorted set.

        Returns:
            priority (Iterable[str]): An iterable of members in the sorted set, sorted by priority.

        """
        yield from self.conn.zrange(self.full_name(key), 0, -1)

    def priority_remove(self, key: str, unique_id: str) -> None:
        """Remove an item from a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The item's (Message's) unique identifier

        Returns:
            None

        """
        self.conn.zrem(
            self.full_name(key),
            unique_id,
        )

    def pub(self, channel: str, value: Any) -> None:
        self.conn.publish(
            channel=channel,
            message=self.serializer.forward(value),
        )

    def sub(self, channel: str) -> Iterable[str | dict[str, Any]]:
        pubsub = self.conn.pubsub()
        pubsub.subscribe(channel)
        try:
            for message in pubsub.listen():
                if message.get("type") == "message" and message.get("data"):
                    yield parse_sub_data(message, serializer=self.serializer)
        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        """Scan the backend for matching names.

        Args:
            pattern (str): pattern to match against

        Returns:
            names_stream (Iterable[str]): a stream of matching name

        """
        match = self.full_name(pattern or "*")
        yield from map(self.short_name, self.conn.scan_iter(match))
