"""

    Disk Backend

"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Type, Union, cast

from alsek import Lock
from alsek._defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient
from alsek.storage.serialization import JsonSerializer, Serializer
from alsek.types import Empty
from alsek.utils.printing import auto_repr
from alsek.utils.string import name_matcher

try:
    from diskcache import Cache as DiskCache
except ImportError:
    raise ImportError("diskcache is not installed.")


class DiskCacheBackend(Backend):
    """DiskCache Backend.

    Backend powered by DiskCache.

    Args:
        conn (str, Path, DiskCache, LazyClient, optional): a directory, ``DiskCache()`` object
            or ``LazyClient``.
        name_match_func (callable, optional): a callable to determine if
            a name matches a specified pattern when scanning
            the backend. If ``None``, one will select automatically.
        namespace (str): prefix to use when inserting
            names in the backend
        serializer (Serializer): tool for encoding and decoding
            values written into the backend.

    Warning:
        ``DiskCache`` persists data to a local (Sqlite) database and does
        not implement 'server-side' "if not exist" on `SET` (`nx`) support or
        true priority capabilities. For these reasons, ``DiskCacheBackend()`` is
        recommended for development and testing purposes only. (Multi-worker setups
        in particular should not be used with this backend.)

    """

    SUPPORTS_PUBSUB: bool = False

    def __init__(
        self,
        conn: Optional[Union[str, Path, DiskCache, LazyClient]] = None,
        name_match_func: Optional[Callable[[Optional[str], str], bool]] = None,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Serializer = JsonSerializer(),
    ) -> None:
        super().__init__(namespace, serializer=serializer)
        self._conn = self._conn_parse(conn)
        self.name_match_func = name_match_func or name_matcher

    @staticmethod
    def _conn_parse(
        conn: Optional[Union[str, Path, DiskCache, LazyClient]]
    ) -> Union[DiskCache, LazyClient, Callable[[], DiskCache]]:
        if isinstance(conn, LazyClient):
            return conn

        if conn is None:
            parsed = DiskCache()
        elif isinstance(conn, DiskCache):
            parsed = conn
        elif isinstance(conn, (str, Path)):
            parsed = DiskCache(str(conn))
        else:
            raise ValueError(f"Unsupported conn {conn}")
        return parsed

    @property
    def conn(self) -> DiskCache:
        """Connection to the backend."""
        if isinstance(self._conn, LazyClient):
            self._conn = self._conn.get()
        return cast(DiskCache, self._conn)

    def __repr__(self) -> str:
        return auto_repr(
            self,
            cache=self.conn,
            key_match_func=self.name_match_func,
            namespace=self.namespace,
            serializer=self.serializer,
        )

    def destroy(self) -> None:
        """Destroy the cache.

        Returns:
            None

        Warning:
            * This method recursively delete the cache directory.
              Use with caution.
            * The backend will not be usable following execution
              of this method.

        """
        shutil.rmtree(self.conn.directory)

    def exists(self, name: str) -> bool:
        """Check if ``name`` exists in the disk backend.

        Args:
            name (str): name of the item

        Returns:
            bool

        """
        return self.full_name(name) in self.conn

    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        """Set ``name`` to ``value`` in the disk backend.

        Args:
            name (str): name of the item
            value (Any): value to set for ``name``
            nx (bool): if ``True`` the item must not exist prior to being set
            ttl (int, optional): time to live for the entry in milliseconds

        Returns:
            None

        Warning:
            * ``nx`` is implement client-side for ``DiskCache``.
               For this reason, it should not be relied upon for
               critical tasks.

        Raises:
            KeyError: if ``nx`` is ``True`` and ``name`` already exists

        """
        if nx and self.exists(name):
            raise KeyError(f"Name '{name}' already exists")

        self.conn.set(
            self.full_name(name),
            value=self.serializer.forward(value),
            expire=ttl if ttl is None else ttl / 1000,
        )

    def get(self, name: str, default: Optional[Union[Any, Type[Empty]]] = None) -> Any:
        """Get ``name`` from the disk backend.

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
        """Delete a ``name`` from the disk backend.

        Args:
            name (str): name of the item
            missing_ok (bool): if ``True``, do not raise for missing

        Returns:
            None

        Raises:
            KeyError: if ``missing_ok`` is ``False`` and ``name`` is not found
                of if ``name`` has a non-expired TTL.

        """
        try:
            self.conn.__delitem__(self.full_name(name), retry=False)
        except KeyError:
            if not missing_ok:
                raise KeyError(f"No name '{name}' found")

    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        """Scan the disk backend for matching names.

        Args:
            pattern (str): pattern to limit search to

        Returns:
            name_stream (Iterable[str]): a stream of matching name

        """
        for name in filter(self.in_namespace, self.conn):
            short_name = self.short_name(name)
            if self.name_match_func(pattern, short_name):
                yield short_name

    def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
        """Add an item to a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The item's (Message's) unique identifier
            priority (float): The numeric priority score (decide if lower or higher means higher priority).

        Returns:
            None

        """
        with Lock(f"mutex_{key}", backend=self) as lock:
            if lock.acquire(strict=False):
                queue = self.get(key, default=[])
                queue = {unique_id: priority for (unique_id, priority) in queue}
                queue[unique_id] = priority
                self.set(
                    key,
                    value=sorted(queue.items(), key=lambda x: x[-1]),
                    nx=False,
                )
            else:
                raise RuntimeError("Could not acquire lock")

    def priority_get(self, key: str) -> Optional[str]:
        """Get (peek) the highest-priority item without removing it.

        Args:
            key (str): The name of the sorted set.

        Returns:
            top key (str, optional): The member with the highest priority, or None if empty.

        """
        if queue := self.get(key):
            (unique_id, _) = min(queue, key=lambda x: (x[-1]))
            return unique_id
        else:
            return None

    def priority_iter(self, key: str) -> Iterable[str]:
        """Iterate over the items in a priority-sorted set.

        Args:
            key (str): The name of the sorted set.

        Returns:
            priority (Iterable[str]): An iterable of members in the sorted set, sorted by priority.

        """
        queue = self.get(key, default=list())
        queue_unique = set(map(tuple, queue))  # ensure unique
        for k, _ in sorted(queue_unique, key=lambda x: x[-1]):
            yield k

    def priority_remove(self, key: str, unique_id: str) -> None:
        """Remove an item from a priority-sorted set.

        Args:
            key (str): The name of the sorted set.
            unique_id (str): The item's (Message's) unique identifier

        Returns:
            None

        """
        with Lock(f"mutex_{key}", backend=self) as lock:
            if lock.acquire(strict=False):
                queue = self.get(key, default=[])
                self.set(
                    key,
                    value=[(u, p) for (u, p) in queue if u != unique_id],
                    nx=False,
                )
            else:
                raise RuntimeError("Could not acquire lock")
