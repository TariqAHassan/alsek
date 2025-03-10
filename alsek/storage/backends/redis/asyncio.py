"""

    Asynchronous Redis Backend

"""

from __future__ import annotations
import logging
from typing import Any, AsyncIterable, Optional, Type, Union

from redis.asyncio import Redis as RedisAsync
from redis.asyncio import ConnectionPool as AsyncConnectionPool

import dill
from alsek.storage.backends import LazyClient, AsyncBackend
from alsek.storage.backends.redis.standard import parse_sub_data
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr

log = logging.getLogger(__name__)


class RedisAsyncBackend(AsyncBackend):
    """Asynchronous Redis Backend.

    This backend is powered by Redis and provides asynchronous support
    for Redis operations.

    Args:
        conn (Optional[Union[str, AsyncRedis, LazyClient]]): A connection URL,
            an `AsyncRedis` instance, or a `LazyClient`. If `None`, a default
            `AsyncRedis` instance is created.
        **kwargs: Additional keyword arguments passed to the base class initializer.

    """

    IS_ASYNC: bool = True

    def __init__(
        self,
        conn: Optional[Union[str, RedisAsync, LazyClient]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._conn = self._conn_parse(conn)

    @staticmethod
    def _conn_parse(
        conn: Optional[Union[str, RedisAsync, LazyClient]]
    ) -> Union[RedisAsync, LazyClient]:
        """Parse the connection parameter to obtain an AsyncRedis or LazyClient instance.

        Args:
            conn (Optional[Union[str, AsyncRedis, LazyClient]]): The connection parameter.

        Returns:
            Union[AsyncRedis, LazyClient]: An `AsyncRedis` instance or a `LazyClient` wrapping one.

        Raises:
            ValueError: If the connection parameter is of an unsupported type.

        """
        if isinstance(conn, LazyClient):
            return conn
        elif conn is None:
            return RedisAsync(decode_responses=True)
        elif isinstance(conn, RedisAsync):
            return conn
        elif isinstance(conn, str):
            return RedisAsync.from_url(conn, decode_responses=True)
        else:
            raise ValueError(f"Unsupported `conn` {conn}")

    @property
    def conn(self) -> RedisAsync:
        """Asynchronous Redis connection.

        Returns:
            AsyncRedis: The asynchronous Redis client instance.

        """
        if isinstance(self._conn, LazyClient):
            self._conn = self._conn.get()
        return self._conn

    def __repr__(self) -> str:
        return auto_repr(
            self,
            conn=self.conn,
            namespace=self.namespace,
            serializer=self.serializer,
        )

    def _encode(self) -> bytes:
        data: dict[str, Any] = dict(
            backend=self.__class__,
            settings=gather_init_params(self, ignore=("conn",)),
        )
        data["settings"]["conn"] = dict(
            connection_class=self.conn.connection_pool.connection_class,
            max_connections=self.conn.connection_pool.max_connections,
            connection_kwargs=self.conn.connection_pool.connection_kwargs,
        )
        return dill.dumps(data)

    @classmethod
    def _from_settings(cls, settings: dict[str, Any]) -> RedisAsync:
        settings["conn"] = RedisAsync(
            connection_pool=AsyncConnectionPool(
                connection_class=settings["conn"]["connection_class"],
                max_connections=settings["conn"]["max_connections"],
                **settings["conn"]["connection_kwargs"],
            )
        )
        return cls(**settings)

    async def exists(self, name: str) -> bool:
        """Check if a key exists in the Redis backend asynchronously.

        Args:
            name (str): The name of the key to check.

        Returns:
            bool: `True` if the key exists, `False` otherwise.

        """
        return await self.conn.exists(self.full_name(name))

    async def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        """Set a value for a key in the Redis backend asynchronously.

        Args:
            name (str): The name of the key.
            value (Any): The value to set.
            nx (bool, optional): If `True`, only set the key if it does not already exist.
            ttl (Optional[int], optional): Time to live for the key in milliseconds.

        Raises:
            KeyError: If `nx` is `True` and the key already exists.

        """
        response = await self.conn.set(
            self.full_name(name),
            value=self.serializer.forward(value),
            px=ttl,
            nx=nx,
            keepttl=ttl is None,
        )
        if nx and not response:
            raise KeyError(f"Name '{name}' already exists")

    async def get(
        self,
        name: str,
        default: Optional[Union[Any, Type[Empty]]] = Empty,
    ) -> Any:
        """Get the value of a key from the Redis backend asynchronously.

        Args:
            name (str): The name of the key.
            default (Optional[Union[Any, Type[Empty]]], optional): Default value if the key does not exist.

        Returns:
            Any: The value of the key.

        Raises:
            KeyError: If the key does not exist and no default is provided.

        """
        return await self._get_engine(
            lambda: self.conn.get(self.full_name(name)),
            default=default,
        )

    async def delete(self, name: str, missing_ok: bool = False) -> None:
        """Delete a key from the Redis backend asynchronously.

        Args:
            name (str): The name of the key to delete.
            missing_ok (bool, optional): If `True`, do not raise an error if the key does not exist.

        Raises:
            KeyError: If the key does not exist and `missing_ok` is `False`.

        """
        found = await self.conn.delete(self.full_name(name))
        if not missing_ok and not found:
            raise KeyError(f"No name '{name}' found")

    async def pub(self, channel: str, value: Any) -> None:
        """Publish a message to a Redis channel asynchronously.

        Args:
            channel (str): The name of the channel.
            value (Any): The message to publish.

        Returns:
            None

        """
        await self.conn.publish(
            channel=channel,
            message=self.serializer.forward(value),
        )

    async def sub(self, channel: str) -> AsyncIterable[dict[str, Any]]:
        """Subscribe to a Redis channel and asynchronously yield messages.

        Args:
            channel (str): The name of the channel to subscribe to.

        Yields:
            dict[str, Any]: A dictionary representing the message data.

        """
        pubsub = self.conn.pubsub()
        await pubsub.subscribe(channel)
        try:
            async for message in pubsub.listen():
                if message.get("type") == "message" and message.get("data"):
                    yield parse_sub_data(message, serializer=self.serializer)
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()

    async def scan(self, pattern: Optional[str] = None) -> AsyncIterable[str]:
        """Asynchronously scan the Redis backend for keys matching a pattern.

        Args:
            pattern (Optional[str], optional): The pattern to match keys against. Defaults to '*'.

        Yields:
            str: The names of matching keys without the namespace prefix.

        """
        match = self.full_name(pattern or "*")
        async for key in self.conn.scan_iter(match):
            yield self.short_name(key)
