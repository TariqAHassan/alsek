"""

    Utils

"""
from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional

import redis_lock

from alsek.storage.backends.redis import RedisBackend


class BaseLockInterface(ABC):
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


class RedisLockInterface(BaseLockInterface):
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
