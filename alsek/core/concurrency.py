"""

    Concurrency

"""
from __future__ import annotations

from socket import gethostname
from types import TracebackType
from typing import Optional, Type, cast

from alsek._utils.printing import auto_repr
from alsek.storage.backends import Backend


class Lock:
    """Distributed mutual exclusion (MUTEX) lock.

    Args:
        name (str): name of the lock
        backend (Backend): backend for data storage
        ttl (int, optional): time to live in milliseconds.
            If ``None``, the lock will not expire automatically.
        auto_release (bool): if ``True`` automatically release
            the lock on context exit.

    Warning:
        * Locks are global and do not consider queues, unless
          included in ``name``.
        * When used as a context manager, the lock is *not* automatically
          acquired. Lock acquisition requires calling ``acquire()``.

    Examples:
        >>> from alsek import Lock
        >>> from alsek.storage.backends.disk import DiskCacheBackend
        ...
        >>> backend = DiskCacheBackend()
        ...
        >>> with Lock("mutex", backend=backend) as lock:
        >>>     if lock.acquire():
        >>>         print("Acquired lock.")
        >>>     else:
        >>>         print("Did not acquire lock.")

    """

    def __init__(
        self,
        name: str,
        backend: Backend,
        ttl: Optional[int] = 60 * 60 * 1000,
        auto_release: bool = True,
    ) -> None:
        self.name = name
        self.backend = backend
        self.ttl = ttl
        self.auto_release = auto_release

    def __repr__(self) -> str:
        return auto_repr(
            self,
            name=self.name,
            backend=self.backend,
            ttl=self.ttl,
            auto_release=self.auto_release,
        )

    @property
    def long_name(self) -> str:
        """Subnamespace for the lock."""
        if self.name.startswith("locks:"):
            return self.name
        else:
            return f"locks:{self.name}"

    @property
    def holder(self) -> Optional[str]:
        """Name of the host that currently holds the lock, if any."""
        return cast(Optional[str], self.backend.get(self.long_name))

    @property
    def held(self) -> bool:
        """If the lock is held by the current host."""
        return self.holder == gethostname()

    def acquire(self, strict: bool = True) -> bool:
        """Try to acquire the lock.

        Args:
            strict (bool): if ``True`` return ``False`` if
                the lock is already held.

        Returns:
            acquired (bool): ``True`` if the message is
                acquired or already acquired by the current
                host.

        Warning:
            * ``True`` is only returned if execution of this method
              resulted in acquisition of the lock. This means that
              ``False`` will be returned if the current host already
              holds the lock.

        """
        if self.held:
            return not strict

        try:
            self.backend.set(self.long_name, value=gethostname(), nx=True, ttl=self.ttl)
            return True
        except KeyError:
            return False

    def release(self) -> bool:
        """Release the lock.

        Returns:
            released (bool): whether or not the lock was
                found and released.

        """
        if self.held:
            self.backend.delete(self.long_name, missing_ok=True)
            return True
        else:
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
