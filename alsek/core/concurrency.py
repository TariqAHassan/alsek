"""

    Concurrency

"""

from __future__ import annotations

import os
import threading
from socket import gethostname
from types import TracebackType
from typing import Optional, Type, cast, Any

import redis_lock
from sympy.core.cache import cached_property

from socket import gethostname
from alsek.storage.backends import Backend
from alsek.storage.backends.redis import RedisBackend
from alsek.utils.printing import auto_repr

# Mutex locks operate at the host-level only.
MY_HOST_LOCK_OWNER_ID = f"lock:{gethostname()}"


def _get_process_lock_owner_id() -> str:
    return f"{MY_HOST_LOCK_OWNER_ID}:{os.getpid()}"


def _get_thread_lock_owner_id() -> str:
    return f"{_get_process_lock_owner_id()}:{threading.get_ident()}"


class Lock:
    """Distributed mutual exclusion (MUTEX) lock for controlling
    concurrency accross machines.

    Args:
        name (str): name of the lock
        backend (Backend): backend for data storage
        ttl (int, optional): time to live in milliseconds.
            If ``None``, the lock will not expire automatically.
        auto_release (bool): if ``True`` automatically release
            the lock on context exit.
        owner_id (str): unique identifier for the lock.
            Do not change this value unless you know what you are doing.

    Warning:
        * Locks are global and do not consider queues, unless
          included in ``name``.
        * When used as a context manager, the lock is *not* automatically
          acquired. Lock acquisition requires calling ``acquire()``.

    Examples:
        >>> from alsek import Lock
        >>> from alsek.storage.backends.redis.standard import RedisBackend
        ...
        >>> backend = RedisBackend()
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
        owner_id: str = MY_HOST_LOCK_OWNER_ID,
    ) -> None:
        self.name = name
        self.backend = backend
        self.ttl = ttl
        self.auto_release = auto_release
        self.owner_id = owner_id

        if not isinstance(backend, RedisBackend):
            raise NotImplementedError("Only RedisBackend is supported.")

        self._lock = redis_lock.Lock(
            backend.conn,
            name=self.full_name,
            expire=None if ttl is None else round(ttl / 1000),
            id=self.owner_id,
        )

    def __repr__(self) -> str:
        return auto_repr(
            self,
            name=self.name,
            backend=self.backend,
            ttl=self.ttl,
            auto_release=self.auto_release,
        )

    @property
    def full_name(self) -> str:
        """The full name of the lock including its namespace prefix."""
        return f"{self.backend.namespace}:{self.name}"

    @property
    def holder(self) -> Optional[str]:
        """Name of the host that currently holds the lock, if any."""
        return self._lock.get_owner_id()

    @property
    def held(self) -> bool:
        """If the lock is held by the current owner."""
        return self.holder == self.owner_id

    def acquire(
        self,
        wait: Optional[int] = None,
        already_acquired_ok: bool = True,
    ) -> bool:
        """Try to acquire the lock.

        Args:
            wait (int, optional): the amount of time wait to acquire
                the lock (in seconds). If ``None`` do not block.
            already_acquired_ok (bool): if ``True`` do not raise if the lock
                is already held by the current owner.

        Returns:
            acquired (bool): ``True`` if the message is
                acquired or already acquired by the current owner.

        """
        try:
            return self._lock.acquire(blocking=bool(wait), timeout=wait)
        except redis_lock.AlreadyAcquired as error:
            if already_acquired_ok:
                return True
            else:
                raise error

    def release(self) -> bool:
        """Release the lock.

        Returns:
            released (bool): whether the lock was
                found and released.

        """
        try:
            self._lock.release()
            return True
        except redis_lock.NotAcquired:
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


class ProcessLock(Lock):
    """Distributed mutual exclusion (MUTEX) lock for controlling
    concurrency accross processes on the same host.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs, owner_id=_get_process_lock_owner_id())


class ThreadLock(Lock):
    """Distributed mutual exclusion (MUTEX) lock for controlling
    concurrency accross processes and threads on the same host.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs, owner_id=_get_thread_lock_owner_id())
