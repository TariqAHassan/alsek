"""

    Utils

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional, Literal

import redis_lock
from sqlalchemy.orm import Session

from alsek.exceptions import LockAlreadyAcquiredError, LockNotAcquiredError
from alsek.storage.backends.postgres.tables import DistributedLock

from alsek.storage.backends.redis import RedisBackend
from alsek.storage.backends.postgres import PostgresBackend
from alsek.utils.temporal import utcnow, compute_expiry_datetime

IF_ALREADY_ACQUIRED_TYPE = Literal["raise_error", "return_true", "return_false"]


class BaseLockInterface(ABC):
    def __init__(
        self,
        name: str,
        namespace: str,
        ttl: Optional[int],
        owner_id: str,  # noqa
    ) -> None:
        self.name = name
        self.namespace = namespace
        self.ttl = ttl
        self.owner_id = owner_id

        self.validate()

    @property
    def lock_id(self) -> str:
        return f"{self.namespace}:{self.name}"

    def validate(self) -> None:
        if not self.name:
            raise ValueError("`name` must be provided.")

    @abstractmethod
    def get_holder_id(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def acquire(
        self,
        if_already_acquired: IF_ALREADY_ACQUIRED_TYPE,
        blocking: bool = True,
        timeout: Optional[int] = None,
    ) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def release(self) -> None:
        raise NotImplementedError()


class RedisLockInterface(BaseLockInterface):
    def __init__(
        self,
        name: str,
        backend: RedisBackend,
        ttl: Optional[int],
        owner_id: str,  # noqa
    ) -> None:
        super().__init__(
            name=name,
            namespace=backend.namespace,
            ttl=ttl,
            owner_id=owner_id,
        )
        self.backend = backend

    @cached_property
    def _engine(self) -> redis_lock.Lock:
        return redis_lock.Lock(
            self.backend.conn,  # noqa
            name=self.lock_id,
            expire=None if self.ttl is None else round(self.ttl / 1000),
            id=self.owner_id,
        )

    def get_holder_id(self) -> str:
        return self._engine.get_owner_id()

    def acquire(
        self,
        if_already_acquired: IF_ALREADY_ACQUIRED_TYPE,
        blocking: bool = True,
        timeout: Optional[int] = None,
    ) -> bool:
        try:
            return self._engine.acquire(
                blocking=blocking,
                timeout=timeout,
            )
        except redis_lock.AlreadyAcquired as error:
            raise LockAlreadyAcquiredError(
                f"Lock {self.lock_id} already acquired"
            ) from error

    def release(self) -> None:
        try:
            self._engine.release()
        except redis_lock.NotAcquired as error:
            raise LockNotAcquiredError(f"Lock '{self.lock_id}' not acquired") from error


class PostgresLockInterface(BaseLockInterface):
    def __init__(
        self,
        name: str,
        backend: PostgresBackend,
        ttl: Optional[int],
        owner_id: str,
    ) -> None:
        super().__init__(
            name=name,
            namespace=backend.namespace,
            ttl=ttl,
            owner_id=owner_id,
        )
        self.backend = backend

    @staticmethod
    def _is_lock_expired(
        lock_record: DistributedLock,
        session: Session,
    ) -> bool:
        if lock_record.expires_at is not None and lock_record.expires_at <= utcnow():
            session.delete(lock_record)
            session.commit()
            return True
        return False

    def get_holder_id(self) -> Optional[str]:
        """Get the current holder of the lock."""
        with self.backend.session() as session:
            lock_record: Optional[DistributedLock] = session.get(
                DistributedLock,
                self.lock_id,
            )
            if lock_record is None:
                return None
            elif self._is_lock_expired(lock_record, session):
                return None
            else:
                return lock_record.owner_id

    def _get_lock_record_for_update(
        self,
        session: Session,
        blocking: bool,
    ) -> Optional[DistributedLock]:
        query = session.query(DistributedLock).filter(
            DistributedLock.id == self.lock_id
        )
        if blocking:
            result = query.with_for_update().one_or_none()
        else:
            result = query.with_for_update(nowait=True).one_or_none()
        return result

    def _can_acquire_lock(self, lock_record: DistributedLock) -> bool:
        if lock_record.expires_at is not None and lock_record.expires_at <= utcnow():
            return True
        elif lock_record.owner_id == self.owner_id:
            return True
        else:
            return False

    def _create_new_lock(self, session: Session) -> bool:
        current_time = utcnow()
        session.add(
            DistributedLock(
                id=self.lock_id,
                owner_id=self.owner_id,
                acquired_at=current_time,
                expires_at=compute_expiry_datetime(self.ttl, current_time=current_time),
            )
        )
        session.commit()
        return True

    def _update_lock_ownership(self, lock_record: DistributedLock) -> None:
        current_time = utcnow()
        lock_record.owner_id = self.owner_id
        lock_record.acquired_at = current_time
        lock_record.expires_at = compute_expiry_datetime(self.ttl, current_time=current_time)

    def acquire(
        self,
        if_already_acquired: IF_ALREADY_ACQUIRED_TYPE,
        blocking: bool = True,
        timeout: Optional[int] = None,
    ) -> bool:
        """Acquire the lock using PostgreSQL row-level locking."""
        with self.backend.session() as session:
            try:
                lock_record = self._get_lock_record_for_update(
                    session=session,
                    blocking=blocking,
                )
                # A. No existing lock, create new one
                if lock_record is None:
                    return self._create_new_lock(session)
                # B. Lock already acquired by same owner
                elif (
                    if_already_acquired == "raise_error"
                    and lock_record.owner_id == self.owner_id
                ):
                    raise LockAlreadyAcquiredError(f"Lock {self.lock_id} already acquired")  # fmt: skip
                # C. Can acquire the lock (expired)
                elif self._can_acquire_lock(lock_record):
                    self._update_lock_ownership(lock_record)
                    session.commit()
                    return True
                # D. Lock is held by someone else
                else:
                    return False
            except Exception as error:
                session.rollback()
                raise error

    def release(self) -> None:
        """Release the lock."""
        with self.backend.session() as session:
            lock_record: Optional[DistributedLock] = (
                session.query(DistributedLock)
                .filter(
                    DistributedLock.id == self.lock_id,
                    DistributedLock.owner_id == self.owner_id,
                )
                .one_or_none()
            )
            if lock_record:
                session.delete(lock_record)
                session.commit()
            else:
                raise LockNotAcquiredError(f"Lock {self.lock_id} was not acquired")
