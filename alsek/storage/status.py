"""

    Status Storage

"""
from enum import Enum
from typing import Optional, Union
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from alsek.core.message import Message
from alsek.core.broker import Broker
from alsek.exceptions import ValidationError
from alsek.storage.backends import Backend


class TaskStatus(Enum):
    """Alsek task statuses."""

    UNKNOWN = 0
    SUBMITTED = 1
    RUNNING = 2
    RETRYING = 3
    FAILED = 4
    SUCCEEDED = 5


TERMINAL_TASK_STATUSES = (TaskStatus.FAILED, TaskStatus.SUCCEEDED)


def _name2message(name: str) -> Message:
    *_, queue, task_name, uuid = name.split(":")
    return Message(task_name, queue=queue, uuid=queue)


class StatusStore:
    """Alsek Status Store.

    Args:
        broker (Broker): broker used by tasks
        ttl (int, optional): time to live (in milliseconds) for the status
        integrity_scan_trigger (CronTrigger, DateTrigger, IntervalTrigger, optional):
            trigger which determines how often to scan for messages with non-terminal
            statuses (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``) that
            no longer exist in the broker. Entries which meet this criteria will have
            their status set to ``TaskStatus.UNKNOWN``.

    """

    def __init__(
        self,
        broker: Broker,
        ttl: Optional[int] = 60 * 60 * 24 * 7 * 1000,
        integrity_scan_trigger: Optional[
            Union[CronTrigger, DateTrigger, IntervalTrigger]
        ] = IntervalTrigger(hours=1),
    ) -> None:
        self.broker = broker
        self.ttl = ttl
        self.integrity_scan_trigger = integrity_scan_trigger

        self.scheduler = BackgroundScheduler()
        if integrity_scan_trigger:
            self.scheduler.start()
            self.scheduler.add_job(
                self._integrity_scan,
                trigger=integrity_scan_trigger,
                id="integrity_scan",
            )

    @property
    def _backend(self) -> Backend:
        return self.broker.backend

    @staticmethod
    def get_storage_name(message: Message) -> str:
        return f"status:{message.queue}:{message.task_name}:{message.uuid}"

    def exists(self, message: Message) -> bool:
        """Check if a status for ``message`` exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            bool

        """
        return self._backend.exists(self.get_storage_name(message))

    def set(self, message: Message, status: TaskStatus) -> None:
        """Set a ``status`` for ``message``.

        Args:
            message (Message): an Alsek message
            status (TaskStatus): a status to set

        Returns:
            None

        """
        self._backend.set(
            self.get_storage_name(message),
            value=status.name,
            ttl=self.ttl if status == TaskStatus.SUBMITTED else None,
        )

    def get(self, message: Message) -> TaskStatus:
        """Get the status of ``message``.

        Args:
            message (Message): an Alsek message

        Returns:
            status (TaskStatus): the status of ``message``

        """
        status_name = self._backend.get(self.get_storage_name(message))
        return TaskStatus[status_name]

    def delete(self, message: Message, check: bool = True) -> None:
        """Delete the status of ``message``.

        Args:
            message (Message): an Alsek message
            check (bool): check that it is safe to delete the status.
                This is done by ensuring that the current status of ``message``
                is terminal (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``).

        Returns:
            None

        Raises:
            ValidationError: if ``check`` is ``True`` and the status of
                ``message`` is not ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``.

        """
        if check and self.get(message) not in TERMINAL_TASK_STATUSES:
            raise ValidationError(f"Message '{message.uuid}' in a non-terminal state")
        self._backend.delete(self.get_storage_name(message), missing_ok=False)

    def _integrity_scan(self) -> None:
        for name in self._backend.scan("status*"):
            message = _name2message(name)
            status = self.get(message)
            if (
                status is not None
                and status not in TERMINAL_TASK_STATUSES
                and not self.broker.exists(message)
            ):
                self.set(message, status=TaskStatus.UNKNOWN)
