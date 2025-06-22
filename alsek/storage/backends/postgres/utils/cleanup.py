"""

    Maintenance

"""
from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine


class BasePostgresCronMaintenance(ABC):
    # noinspection SqlDialectInspection
    __CHECK_EXTENSION_SQL__ = "SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'"

    # noinspection SqlDialectInspection
    __CREATE_EXTENSION_SQL__ = "CREATE EXTENSION IF NOT EXISTS pg_cron"

    # noinspection SqlDialectInspection
    __CLEANUP_SQL__ = """
    DELETE FROM alsek.key_value 
    WHERE expires_at IS NOT NULL 
    AND expires_at <= (now() AT TIME ZONE 'UTC')
    """

    __JOB_NAME__: str = "alsek_cleanup_expired_keyvalue"
    __UNSCHEDULE_SQL__: str = "SELECT cron.unschedule(:job_name)"
    __SCHEDULE_SQL__: str = "SELECT cron.schedule(:job_name, :schedule, :command)"

    def __init__(self, engine: Engine | AsyncEngine, interval_seconds: int = 300) -> None:
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
        raise NotImplementedError

    @abstractmethod
    def create_pg_cron_extension(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def setup_cleanup_job(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def remove_cleanup_job(self) -> bool:
        raise NotImplementedError


class PostgresCronMaintenance(BasePostgresCronMaintenance):
    def __init__(self, engine: Engine, interval_seconds: int = 300) -> None:
        super().__init__(engine, interval_seconds)

    def has_pg_cron_extension(self) -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(self.__CHECK_EXTENSION_SQL__))
                return result.fetchone() is not None
        except SQLAlchemyError:
            return False

    def create_pg_cron_extension(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(self.__CREATE_EXTENSION_SQL__))
                conn.commit()
                return True
        except SQLAlchemyError:
            return False

    def setup_cleanup_job(self) -> bool:
        if not self.has_pg_cron_extension():
            if not self.create_pg_cron_extension():
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

        except SQLAlchemyError as e:
            print(f"Failed to setup pg_cron cleanup job: {e}")
            return False

    def remove_cleanup_job(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )
                conn.commit()
                return True
        except SQLAlchemyError as e:
            print(f"Failed to remove pg_cron cleanup job: {e}")
            return False


class PostgresCronMaintenanceAsync(BasePostgresCronMaintenance):
    def __init__(self, engine: AsyncEngine, interval_seconds: int = 300) -> None:
        super().__init__(engine, interval_seconds)

    async def has_pg_cron_extension(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text(self.__CHECK_EXTENSION_SQL__))
                return result.fetchone() is not None
        except SQLAlchemyError:
            return False

    async def create_pg_cron_extension(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text(self.__CREATE_EXTENSION_SQL__))
                await conn.commit()
                return True
        except SQLAlchemyError:
            return False

    async def setup_cleanup_job(self) -> bool:
        if not await self.has_pg_cron_extension():
            if not await self.create_pg_cron_extension():
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

        except SQLAlchemyError as e:
            print(f"Failed to setup pg_cron cleanup job: {e}")
            return False

    async def remove_cleanup_job(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(
                    text(self.__UNSCHEDULE_SQL__),
                    {"job_name": self.__JOB_NAME__},
                )
                await conn.commit()
                return True
        except SQLAlchemyError as e:
            print(f"Failed to remove pg_cron cleanup job: {e}")
            return False
