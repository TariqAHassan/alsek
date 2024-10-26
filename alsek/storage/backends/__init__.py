"""

    Backend

"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Optional, Type, Union, cast

import dill

from alsek._defaults import DEFAULT_NAMESPACE
from alsek.storage.serialization import JsonSerializer, Serializer
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.printing import auto_repr

log = logging.getLogger(__name__)


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

    SUPPORTS_PUBSUB: bool = False

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        serializer: Serializer = JsonSerializer(),
    ) -> None:
        self.namespace = namespace
        self.serializer = serializer

    def __repr__(self) -> str:
        return auto_repr(self, namespace=self.namespace, serializer=self.serializer)

    def _encode(self) -> bytes:
        data = dict(backend=self.__class__, settings=gather_init_params(self))
        return cast(bytes, dill.dumps(data))

    @classmethod
    def _from_settings(cls, settings: dict[str, Any]) -> Backend:
        return cls(**settings)

    def in_namespace(self, name: str) -> bool:
        """Determine if ``name`` belong to the current namespace.

        Args:
            name (str): a name (key)

        Returns:
            bool

        Warning:
            * ``name`` should be a complete (i.e., _full_) name.

        """
        return name.startswith(f"{self.namespace}:")

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

        Notes:
            * If ``name`` is already the short name, this method
              will collapse to a no-op.

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
            nx (bool): if ``True`` the item must not exist prior to being set
            ttl (int, optional): time to live for the entry in milliseconds

        Returns:
            None

        Raises:
            KeyError: if ``nx`` is ``True`` and ``name`` already exists

        """
        raise NotImplementedError()

    def _get_engine(
        self,
        getter: Callable[[], Any],
        default: Optional[Union[Any, Type[Empty]]],
    ) -> Any:
        try:
            return self.serializer.reverse(getter())
        except KeyError as error:
            if default is Empty or isinstance(default, Empty):
                raise error
            return default

    @abstractmethod
    def get(self, name: str, default: Optional[Union[Any, Type[Empty]]] = None) -> Any:
        """Get ``name`` from the backend.

        Args:
            name (str): name of the item
            default (Any, Type[Empty], optional): default value for ``name``

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

    def pub(self, channel: str, value: Any) -> None:
        raise NotImplementedError()

    def sub(self, channel: str) -> Iterable[str | dict[str, Any]]:
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

    def clear_namespace(self, raise_on_error: bool = True) -> int:
        """Clear all items in backend under the current namespace.

        Args:
             raise_on_error (bool): raise if a delete operation fails

        Returns:
            count (int): number of items cleared

        Raises:
            KeyError: if ``raise_on_error`` and a delete operation fails

        """
        count: int = 0
        for name in self.scan():
            try:
                self.delete(name, missing_ok=False)
                count += 1
            except KeyError as error:
                if raise_on_error:
                    raise error
                else:
                    log.warning("Unable to delete %r", name)
        return count
