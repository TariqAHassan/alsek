"""

    Redis Backend

"""
from __future__ import annotations
import dill
from typing import Any, Iterable, Optional, Union, cast, Dict

from redis import Redis, ConnectionPool

from alsek._defaults import DEFAULT_NAMESPACE
from alsek._utils.printing import auto_repr
from alsek.storage.backends import Backend, LazyClient
from alsek._utils.aggregation import gather_init_params
from alsek.storage.serialization import JsonSerializer, Serializer


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
        """Serialize the current backend.

        Returns:
            encoded_conn (bytes): a byte representation of the
                current connection.

        """
        data = dict(
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
    def decode(cls, settings: Dict[str, Any]) -> RedisBackend:
        """Reconstruct the backend from settings.

        Args:
            encoded_backend (dict): the output of ``encode_conn``

        Returns:
            backend (RedisBackend): a reconstructed backend

        """
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

    def get(self, name: str) -> Any:
        """Get ``name`` from the Redis backend.

        Args:
            name (str): name of the item

        Returns:
            Any

        """
        return self.serializer.reverse(self.conn.get(self.full_name(name)))

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

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        """Scan the backend for matching names.

        Args:
            pattern (str): pattern to match against

        Returns:
            names_stream (Iterable[str]): a stream of matching name

        """
        match = self.full_name(pattern or "*")
        yield from map(self.short_name, self.conn.scan_iter(match))
