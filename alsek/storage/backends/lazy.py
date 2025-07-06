"""

    Lazy

"""
from __future__ import annotations

from typing import Callable, Any


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
