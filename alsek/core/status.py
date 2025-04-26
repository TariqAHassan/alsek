"""

    Status Tracking

"""

from __future__ import annotations
from enum import Enum
import dill
from typing import Any, Iterable, NamedTuple, Optional, Union

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek.defaults import DEFAULT_TTL
from alsek.core.broker import Broker
from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.storage.backends import Backend
from alsek.utils.aggregation import gather_init_params


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

    """

    def __init__(
        self,
        broker: Broker,
        ttl: Optional[int] = DEFAULT_TTL,
        enable_pubsub: Optional[bool] = None,
    ) -> None:
        self.broker = broker
        self.ttl = ttl
        self.enable_pubsub = broker.backend.SUPPORTS_PUBSUB if enable_pubsub is None else enable_pubsub  # fmt: skip

        if enable_pubsub and not broker.backend.SUPPORTS_PUBSUB:
            raise AssertionError("Backend of broker does not support PUBSUB")

    @property
    def backend(self) -> Backend:
        return self.broker.backend

    def serialize(self) -> dict[str, Any]:
        return {
            "broker": gather_init_params(self.broker, ignore=("backend",)),
            "broker_backend": self.backend.encode(),
            "ttl": self.ttl,
            "enable_pubsub": self.enable_pubsub,
        }

    @staticmethod
    def deserialize(data: dict[str, Any]) -> StatusTracker:
        backend_data = dill.loads(data["broker_backend"])
        backend = backend_data["backend"]._from_settings(backend_data["settings"])
        broker = Broker(backend=backend, **data["broker"])
        return StatusTracker(
            broker=broker,
            ttl=data["ttl"],
            enable_pubsub=data["enable_pubsub"],
        )

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
        return self.backend.exists(self.get_storage_name(message))

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
                    status=TaskStatus[i["data"]["status"]],  # noqa
                    details=i["data"]["details"],  # noqa
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
        self.backend.set(
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
        if value := self.backend.get(self.get_storage_name(message)):
            return StatusUpdate(
                status=TaskStatus[value["status"]],  # noqa
                details=value["details"],
            )
        else:
            raise KeyError(f"No status found for message '{message.summary}'")

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
        self.backend.delete(self.get_storage_name(message), missing_ok=False)


class StatusTrackerIntegryScanner:
    """Tool to ensure the integrity of statuses scanning a ``StatusTracker()``
    with non-terminal statuses (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``)
    that no longer exist in the broker. Entries which meet this criteria will have
    their status set to ``TaskStatus.UNKNOWN``.

    Args:
        status_tracker (StatusTracker): status tracker to scan for messages with non-terminal status
        trigger (CronTrigger, DateTrigger, IntervalTrigger, optional):
            trigger which determines how often to perform the scan.

    """

    def __init__(
        self,
        status_tracker: StatusTracker,
        trigger: Union[CronTrigger, DateTrigger, IntervalTrigger] = IntervalTrigger(hours=1),  # fmt: skip
    ) -> None:
        self.status_tracker = status_tracker
        self.trigger = trigger

        self.scheduler: BackgroundScheduler = BackgroundScheduler()
        if trigger:
            self.scheduler.start()
            self.scheduler.add_job(
                self.scan,
                trigger=trigger,
                id="integrity_scan",
            )

    def scan(self) -> None:
        """Run the integrity scan.

        Returns:
            None

        """
        for name in self.status_tracker.backend.scan("status*"):
            message = _name2message(name)
            status = self.status_tracker.get(message).status
            if (
                status is not None
                and status not in TERMINAL_TASK_STATUSES
                and not self.status_tracker.broker.exists(message)
            ):
                self.status_tracker.set(message, status=TaskStatus.UNKNOWN)
