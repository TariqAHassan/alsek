"""

    Status Tracking

"""

from enum import Enum
from typing import Any, Iterable, NamedTuple, Optional, Union

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek._defaults import DEFAULT_TTL
from alsek.core.broker import Broker
from alsek.core.message import Message
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


class StatusUpdate(NamedTuple):
    """Status information."""

    status: TaskStatus
    details: Optional[Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(
            status=self.status.name,
            details=self.details,
        )


def _name2message(name: str) -> Message:
    *_, queue, task_name, uuid = name.split(":")
    return Message(task_name, queue=queue, uuid=uuid)


class StatusTracker:
    """Alsek Status Tracker.

    Args:
        broker (Broker): broker used by tasks.
        ttl (int, optional): time to live (in milliseconds) for the status
        enable_pubsub (bool, optional): if ``True`` automatically publish PUBSUB updates.
            If ``None`` determine automatically given the capabilities of the backend
            used by ``broker``.
        integrity_scan_trigger (CronTrigger, DateTrigger, IntervalTrigger, optional):
            trigger which determines how often to scan for messages with non-terminal
            statuses (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``) that
            no longer exist in the broker. Entries which meet this criteria will have
            their status set to ``TaskStatus.UNKNOWN``.

    """

    def __init__(
        self,
        broker: Broker,
        ttl: Optional[int] = DEFAULT_TTL,
        enable_pubsub: Optional[bool] = None,
        integrity_scan_trigger: Optional[
            Union[CronTrigger, DateTrigger, IntervalTrigger]
        ] = IntervalTrigger(hours=1),
    ) -> None:
        self.broker = broker
        self.ttl = ttl
        self.enable_pubsub = broker.backend.SUPPORTS_PUBSUB if enable_pubsub is None else enable_pubsub  # fmt: skip
        self.integrity_scan_trigger = integrity_scan_trigger

        if enable_pubsub and not broker.backend.SUPPORTS_PUBSUB:
            raise AssertionError("Backend of broker does not support PUBSUB")

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
        """Get the key for the status information about the message

        Args:
            message (Message): an Alsek message

        Returns:
            name (string): the key for the status information

        """
        if not message.queue or not message.task_name or not message.uuid:
            raise ValidationError("Required attributes not set for message")
        return f"status:{message.queue}:{message.task_name}:{message.uuid}"

    @staticmethod
    def get_pubsub_name(message: Message) -> str:
        """Get the channel for status updates about the message.

        Args:
            message (Message): an Alsek message

        Returns:
            name (string): the channel for the status information

        """
        if not message.queue or not message.task_name or not message.uuid:
            raise ValidationError("Required attributes not set for message")
        return f"channel:{message.queue}:{message.task_name}:{message.uuid}"

    def exists(self, message: Message) -> bool:
        """Check if a status for ``message`` exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            bool

        """
        return self._backend.exists(self.get_storage_name(message))

    def publish_update(self, message: Message, update: StatusUpdate) -> None:
        """Publish a PUBSUB update for a message.

        Args:
            message (Message): an Alsek message
            update (StatusUpdate): a status to publish

        Returns:
            None

        """
        self.broker.backend.pub(
            self.get_pubsub_name(message),
            value=update.as_dict(),  # converting to dict makes this serializer-agnostic
        )

    def listen_to_updates(
        self,
        message: Message,
        auto_exit: bool = True,
    ) -> Iterable[StatusUpdate]:
        """Listen to PUBSUB updates for ``message``.

        Args:
            message (Message): an Alsek message
            auto_exit (bool): if ``True`` stop listening if a terminal status for the
                task is encountered (succeeded or failed).

        Returns:
            stream (Iterable[StatusUpdate]): A stream of updates from the pubsub channel

        """
        if not self.enable_pubsub:
            raise ValueError("PUBSUB not enabled")

        for i in self.broker.backend.sub(self.get_pubsub_name(message)):
            if i.get("type", "").lower() == "message":
                update = StatusUpdate(
                    status=TaskStatus[i["data"]["status"]],
                    details=i["data"]["details"],
                )
                yield update
                if auto_exit and update.status in TERMINAL_TASK_STATUSES:
                    break

    def set(
        self,
        message: Message,
        status: TaskStatus,
        details: Optional[Any] = None,
    ) -> None:
        """Set a ``status`` for ``message``.

        Args:
            message (Message): an Alsek message
            status (TaskStatus): a status to set
            details (Any, optional): additional information about the status (e.g., progress percentage)

        Returns:
            None

        """
        update = StatusUpdate(status=status, details=details)
        self._backend.set(
            self.get_storage_name(message),
            value=update.as_dict(),
            ttl=self.ttl if status == TaskStatus.SUBMITTED else None,
        )
        if self.enable_pubsub:
            self.publish_update(message, update=update)

    def get(self, message: Message) -> StatusUpdate:
        """Get the status of ``message``.

        Args:
            message (Message): an Alsek message

        Returns:
            status (StatusUpdate): the status of ``message``

        """
        value = self._backend.get(self.get_storage_name(message))
        return StatusUpdate(
            status=TaskStatus[value["status"]],
            details=value["details"],
        )

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
        if check and self.get(message).status not in TERMINAL_TASK_STATUSES:
            raise ValidationError(f"Message '{message.uuid}' in a non-terminal state")
        self._backend.delete(self.get_storage_name(message), missing_ok=False)

    def _integrity_scan(self) -> None:
        for name in self._backend.scan("status*"):
            message = _name2message(name)
            status = self.get(message).status
            if (
                status is not None
                and status not in TERMINAL_TASK_STATUSES
                and not self.broker.exists(message)
            ):
                self.set(message, status=TaskStatus.UNKNOWN)
