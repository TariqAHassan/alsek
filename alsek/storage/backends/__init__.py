"""

    Backend

"""
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Optional

from alsek import DEFAULT_NAMESPACE
from alsek._utils.printing import auto_repr
from alsek.storage.serialization import JsonSerializer, Serializer


class LazyClient:
    """Lazy client.

    Wrapper for lazy client initialization.

    Args:
        client_func (callable): a callable which returns
            a backend client.

    """

    def __init__(self, client_func: Callable[[], Any]) -> None:
        self.client_func = client_func

    def get(self) -> Any:
        """Execute ``client_func``.

        Returns:
            client (Any): a backend client

        """
        return self.client_func()


class Backend(ABC):
    """Backend base class.

    Args:
        namespace (str): prefix to use when inserting
            names in the backend
        serializer (Serializer): tool for encoding and decoding
            values written into the backend.

    """

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Serializer = JsonSerializer(),
    ) -> None:
        self.namespace = namespace
        self.serializer = serializer

    def __repr__(self) -> str:
        return auto_repr(self, namespace=self.namespace, serializer=self.serializer)

    def full_name(self, name: str) -> str:
        """Get an item's complete name, including the namespace
        in which it exists.

        Args:
            name (str): the name of an item

        Returns:
            full_name (str): a name of the form ``"{NAMESPACE}:{name}"``

        Notes:
            * If ``name`` is already the full name, this method
              will collapse to a no-op.

        """
        if name.startswith(f"{self.namespace}:"):
            return name
        else:
            return f"{self.namespace}:{name}"

    def short_name(self, name: str) -> str:
        """Get an item's short name, without the namespace
        in which it exists.

        Args:
            name (str): the full name of an item

        Returns:
            short_name (str): ``name`` without the namespace prefix

        """
        return re.sub(rf"^{self.namespace}:", repl="", string=name)

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if ``name`` exists in the backend.

        Args:
            name (str): name of the item

        Returns:
            bool

        """
        raise NotImplementedError()

    @abstractmethod
    def set(
        self,
        name: str,
        value: Any,
        nx: bool = False,
        ttl: Optional[int] = None,
    ) -> None:
        """Set ``name`` to ``value`` in the backend.

        Args:
            name (str): name of the item
            value (Any): value to set for ``name``
            nx (bool): whether or not the item must not exist prior to being set
            ttl (int, optional): time to live for the entry in milliseconds

        Returns:
            None

        Raises:
            KeyError: if ``nx`` is ``True`` and ``name`` already exists

        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, name: str) -> Any:
        """Get ``name`` from the backend.

        Args:
            name (str): name of the item

        Returns:
            Any

        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, name: str, missing_ok: bool = False) -> None:
        """Delete a ``name`` from the backend.

        Args:
            name (str): name of the item
            missing_ok (bool): if ``True``, do not raise for missing

        Returns:
            None

        Raises:
            KeyError: if ``missing_ok`` is ``False`` and ``name`` is not found.

        """
        raise NotImplementedError()

    @abstractmethod
    def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
        """Scan the backend for matching names.

        Args:
            pattern (str, optional): pattern to limit search to

        Returns:
            matches_stream (Iterable[str]): a stream of matching name

        """
        raise NotImplementedError()

    def count(self, pattern: Optional[str] = None) -> int:
        """Count the number of items in the backend.

        Args:
            pattern (str, optional): pattern to limit count to

        Returns:
            count (int): number of matching names

        """
        return sum(1 for _ in self.scan(pattern))

    def clear_namespace(self) -> int:
        """Clear all items in backend under the current namespace.

        Returns:
            count (int): number of items cleared

        """
        count: int = 0
        for name in self.scan():
            self.delete(name)
            count += 1
        return count
