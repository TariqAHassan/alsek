"""

    Concurrency

"""
from __future__ import annotations

from socket import gethostname
from types import TracebackType
from typing import Optional, Type

from alsek._utils.printing import auto_repr
from alsek.storage.backends import Backend


class Lock:
    """Distributed concurrency lock.

    Args:
        name (str): name of the lock
        backend (Backend): backend for data storage
        ttl (int, optional): time to live in milliseconds.
            If ``None``, the lock will not expire automatically.
        limit (int): the maximum number of hosts
            permitted to hold the lock at any one time.
        auto_release (bool): if ``True`` automatically release
            the lock on context exit.

    Notes:
        * Use ``limit=1`` (the default) to obtained a
          mutual exclusion (MUTEX) lock.

    Warning:
        * Locks are global and do not consider queues, unless
          included in ``name``.

    """

    def __init__(
        self,
        name: str,
        backend: Backend,
        ttl: Optional[int] = 60 * 60 * 1000,
        limit: int = 1,
        auto_release: bool = True,
    ) -> None:
        self.name = name
        self.backend = backend
        self.ttl = ttl
        self.limit = limit
        self.auto_release = auto_release

        if limit < 1:
            raise ValueError("`limit` must be greater than one")

    def __repr__(self) -> str:
        return auto_repr(
            self,
            name=self.name,
            backend=self.backend,
            ttl=self.ttl,
            limit=self.limit,
            auto_release=self.auto_release,
        )

    @property
    def subnamespace(self) -> str:
        """Subnamespace for the lock."""
        return f"locks:{self.name}"

    @property
    def long_name(self) -> str:
        """Long name of lock, including the subnamespace and current host."""
        return f"{self.subnamespace}:{gethostname()}"

    @property
    def acquired(self) -> bool:
        """Whether or not the host currently holds the lock."""
        return self.backend.exists(self.long_name)

    def acquire(self) -> bool:
        """Try to acquire the lock.

        Returns:
            acquired (bool): ``True`` if the message is
                acquired or already acquired by the current
                host.

        """
        # ToDo: switch to pessimistic mechanism
        if self.acquired:
            return False
        elif self.backend.count(self.subnamespace) < self.limit:
            self.backend.set(self.long_name, value=None, ttl=self.ttl)
            return True
        else:
            return False

    def release(self) -> bool:
        """Release the lock.

        Returns:
            released (bool): whether or not the lock was
                found and released.

        """
        try:
            self.backend.delete(self.long_name, missing_ok=False)
            return True
        except KeyError:
            return False

    def __enter__(self) -> Lock:
        """Enter the context and try to acquire the lock.

        Returns:
          lock (Lock): underlying lock object.

        """
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit the context. If ``auto_release`` is enabled,
         the lock will be released.

        Args:
            exc_val (BaseException, optional): an exception from within the context
            exc_val (BaseException, optional): value of any exception from
                within the context
            exc_tb (TracebackType, optional): the traceback from the context

        Returns:
            None

        """
        if self.auto_release:
            self.release()
