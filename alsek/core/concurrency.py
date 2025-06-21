"""

    Concurrency

"""

from __future__ import annotations

import os
import threading
from functools import cached_property
from socket import gethostname
from types import TracebackType
from abc import ABC, abstractmethod
from typing import Literal, Optional, Type, get_args, Callable, Any

import redis_lock

from alsek.storage.backends import Backend
from alsek.storage.backends.redis import RedisBackend
from alsek.utils.printing import auto_repr

IF_ALREADY_ACQUIRED_TYPE = Literal["raise_error", "return_true", "return_false"]

CURRENT_HOST_OWNER_ID = f"lock:{gethostname()}"


def _get_process_lock_owner_id() -> str:
    return f"{CURRENT_HOST_OWNER_ID}:process:{os.getpid()}"


def _get_thread_lock_owner_id() -> str:
    return f"{_get_process_lock_owner_id()}:thread:{threading.get_ident()}"


def _resolve_owner_id(owner_id: str | Callable[[], str]) -> str:
    if isinstance(owner_id, str):
        return owner_id
    elif callable(owner_id):
        result = owner_id()
        if isinstance(result, str):
            return result
        else:
            raise TypeError(f"owner_id callable returned invalid result '{result}'")
    else:
        raise TypeError(f"Unable to handle type '{type(owner_id)}'")


class LockInterface(ABC):
    def __init__(
        self,
        name: str,
        backend: RedisBackend,
        ttl: Optional[int],
        owner_id: str,  # noqa
    ) -> None:
        self.name = name
        self.backend = backend
        self.ttl = ttl
        self.owner_id = owner_id

        self.validate()

    @property
    def full_name(self) -> str:
        return f"{self.backend.namespace}:{self.name}"

    def validate(self) -> None:
        if not self.name:
            raise ValueError("`name` must be provided.")

    @abstractmethod
    def get_holder_id(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def acquire(self, blocking: bool = True, timeout: Optional[int] = None) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def release(self) -> None:
        raise NotImplementedError()


class RedisLockInterface(LockInterface):
    @cached_property
    def _engine(self) -> redis_lock.Lock:
        return redis_lock.Lock(
            self.backend.conn,
            name=self.full_name,
            expire=None if self.ttl is None else round(self.ttl / 1000),
            id=self.owner_id,
        )

    def get_holder_id(self) -> str:
        return self._engine.get_owner_id()

    def acquire(self, blocking: bool = True, timeout: Optional[int] = None) -> bool:
        return self._engine.acquire(
            blocking=blocking,
            timeout=timeout,
        )

    def release(self) -> None:
        self._engine.release()


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
        owner_id: str = CURRENT_HOST_OWNER_ID,
    ) -> None:
        self.name = name
        self.backend = backend
        self.ttl = ttl
        self.auto_release = auto_release
        self._owner_id = owner_id

        if isinstance(backend, RedisBackend):
            self.lock_interface = RedisLockInterface(
                name=name,
                backend=backend,
                ttl=ttl,
                owner_id=self.owner_id,
            )
        else:
            raise NotImplementedError(f"Unsupported backend '{backend}'")

    def __repr__(self) -> str:
        return auto_repr(
            self,
            name=self.name,
            backend=self.backend,
            ttl=self.ttl,
            auto_release=self.auto_release,
        )

    @property
    def owner_id(self) -> str:
        """The ID of the owner of the lock."""
        return self._owner_id

    @property
    def full_name(self) -> str:
        """The full name of the lock including its namespace prefix."""
        return f"{self.backend.namespace}:{self.name}"

    @property
    def holder(self) -> Optional[str]:
        """Name of the owner that currently holds the lock, if any."""
        return self.lock_interface.get_holder_id()

    @property
    def held(self) -> bool:
        """If the lock is held by the current owner."""
        return self.holder == self.owner_id

    def acquire(
        self,
        wait: Optional[int] = None,
        if_already_acquired: IF_ALREADY_ACQUIRED_TYPE = "raise_error",
    ) -> bool:
        """Try to acquire the lock.

        Args:
            wait (int, optional): the amount of time wait to acquire
                the lock (in seconds). If ``None`` do not block.
            if_already_acquired (str): if ``True`` do not raise if the lock
                is already held by the current owner.

        Returns:
            acquired (bool): ``True`` if the message is
                acquired or already acquired by the current owner.

        """
        if if_already_acquired not in get_args(IF_ALREADY_ACQUIRED_TYPE):
            raise ValueError(f"Invalid `on_already_acquired`, got  {if_already_acquired}")  # fmt: skip

        try:
            return self.lock_interface.acquire(blocking=bool(wait), timeout=wait)
        except redis_lock.AlreadyAcquired as error:
            if if_already_acquired == "return_true":
                return True
            elif if_already_acquired == "return_false":
                return False
            else:
                raise error

    def release(self, raise_if_not_acquired: bool = False) -> bool:
        """Release the lock.

        Args:
            raise_if_not_acquired (bool): raise if the lock was not
                acquired for release.

        Returns:
            released (bool): whether the lock was
                found and released.

        """
        try:
            self.lock_interface.release()
            return True
        except redis_lock.NotAcquired as error:
            if raise_if_not_acquired:
                raise error
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


class ProcessLock(Lock):
    """Distributed mutual exclusion (MUTEX) lock for controlling
    concurrency across processes on the same host.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs, owner_id=_get_process_lock_owner_id())

    def validate(self) -> None:
        if not self.name:
            raise ValueError("`name` must be provided.")

    @property
    def owner_id(self) -> str:
        # We compute this "fresh" every time so that
        # It's always accurate even if the lock is moved
        # to a different process than it was created in.
        return _get_process_lock_owner_id()


class ThreadLock(Lock):
    """Distributed mutual exclusion (MUTEX) lock for controlling
    concurrency across processes and threads on the same host.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs, owner_id=_get_process_lock_owner_id())

    def validate(self) -> None:
        if not self.name:
            raise ValueError("`name` must be provided.")

    @property
    def owner_id(self) -> str:
        # We compute this "fresh" every time so that
        # It's always accurate even if the lock is moved
        # to a different thread than it was created in.
        return _get_thread_lock_owner_id()
