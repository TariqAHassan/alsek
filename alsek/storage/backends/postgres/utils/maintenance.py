"""

    Maintenance

"""

from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
import logging

from alsek.defaults import DEFAULT_NAMESPACE

log = logging.getLogger(__name__)

NO_PG_CRON_ERROR_MESSAGE = (
    "Failed to create pg_cron extension, unable to automatically remove expired entries. "
    "Please Install pg_cron manually."
)


class BasePostgresCronMaintenance(ABC):
    # noinspection SqlDialectInspection
    __CHECK_DB_HAS_EXTENSION_SQL__ = (
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'"
    )

    # noinspection SqlDialectInspection
    __CREATE_DB_EXTENSION_SQL__ = "CREATE EXTENSION IF NOT EXISTS pg_cron"

    # noinspection SqlDialectInspection
    __CLEANUP_SQL__ = f"""
    DELETE FROM {DEFAULT_NAMESPACE}.key_value 
    WHERE expires_at IS NOT NULL 
    AND expires_at <= (now() AT TIME ZONE 'UTC')
    """

    __JOB_NAME__: str = f"{DEFAULT_NAMESPACE}_cleanup_expired_keyvalue"
    __UNSCHEDULE_SQL__: str = "SELECT cron.unschedule(:job_name)"
    __SCHEDULE_SQL__: str = "SELECT cron.schedule(:job_name, :schedule, :command)"

    def __init__(self, engine: Engine | AsyncEngine, interval_seconds: int = 1) -> None:
        self.engine = engine
        self.interval_seconds = interval_seconds

    def _get_cron_schedule(self) -> str:
        if self.interval_seconds < 60:
            # For sub-minute intervals, use: */N * * * * * (every N seconds)
            return f"*/{self.interval_seconds} * * * * *"
        else:
            # For minute+ intervals, convert to minutes: */N * * * * (every N minutes)
            minutes = self.interval_seconds // 60
            return f"*/{minutes} * * * *"

    @abstractmethod
    def has_pg_cron_extension(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def create_pg_cron_extension(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def create(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def remove(self) -> bool:
        raise NotImplementedError()


class PostgresCronMaintenance(BasePostgresCronMaintenance):
    def has_pg_cron_extension(self) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(self.__CHECK_DB_HAS_EXTENSION_SQL__))
                return result.fetchone() is not None
        except SQLAlchemyError:
            return False

    def create_pg_cron_extension(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(self.__CREATE_DB_EXTENSION_SQL__))
                conn.commit()
                return True
        except SQLAlchemyError:
            return False

    def create(self) -> bool:
        if not self.has_pg_cron_extension():
            if not self.create_pg_cron_extension():
                log.error(NO_PG_CRON_ERROR_MESSAGE)
                return False

        try:
            with self.engine.connect() as conn:
                # First, unschedule any existing job with the same name
                conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )

                # Schedule the new job
                conn.execute(
                    text(self.__SCHEDULE_SQL__),
                    {
                        "job_name": self.__JOB_NAME__,
                        "schedule": self._get_cron_schedule(),
                        "command": self.__CLEANUP_SQL__,
                    },
                )

                conn.commit()
                return True
        except SQLAlchemyError:
            log.error(f"Failed to setup pg_cron cleanup job", exc_info=True)
            return False

    def remove(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )
                conn.commit()
                return True
        except SQLAlchemyError:
            log.error(f"Failed to remove pg_cron cleanup job", exc_info=True)
            return False


class PostgresCronMaintenanceAsync(BasePostgresCronMaintenance):
    async def has_pg_cron_extension(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text(self.__CHECK_DB_HAS_EXTENSION_SQL__))
                return result.fetchone() is not None
        except SQLAlchemyError:
            return False

    async def create_pg_cron_extension(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text(self.__CREATE_DB_EXTENSION_SQL__))
                await conn.commit()
                return True
        except SQLAlchemyError:
            return False

    async def create(self) -> bool:
        if not await self.has_pg_cron_extension():
            if not await self.create_pg_cron_extension():
                log.error(NO_PG_CRON_ERROR_MESSAGE)
                return False

        try:
            async with self.engine.connect() as conn:
                # First, unschedule any existing job with the same name
                await conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )

                # Schedule the new job
                await conn.execute(
                    text(self.__SCHEDULE_SQL__),
                    {
                        "job_name": self.__JOB_NAME__,
                        "schedule": self._get_cron_schedule(),
                        "command": self.__CLEANUP_SQL__,
                    },
                )

                await conn.commit()
                return True

        except SQLAlchemyError:
            log.error(f"Failed to setup pg_cron cleanup job", exc_info=True)
            return False

    async def remove(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )
                await conn.commit()
                return True
        except SQLAlchemyError:
            log.error(f"Failed to remove pg_cron cleanup job", exc_info=True)
            return False
