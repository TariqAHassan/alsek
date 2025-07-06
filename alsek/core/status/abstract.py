"""

    Abstract Status

"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, AsyncIterable, Iterable

from alsek import Message
from alsek.core.status.types import StatusUpdate, TaskStatus
from alsek.defaults import DEFAULT_TTL
from alsek.storage.backends.abstract import BaseBackend
from alsek.exceptions import ValidationError


class BaseStatusTracker(ABC):
    """Alsek Status Tracker.

    Args:
        backend (BaseBackend): backend to persists results to. (In almost all cases, this
            should be the same backend used by the Broker).
        ttl (int, optional): time to live (in milliseconds) for the status
        enable_pubsub (bool, optional): if ``True`` automatically publish PUBSUB updates.
            If ``None`` determine automatically given the capabilities of the backend
            used by ``broker``.

    """

    def __init__(
        self,
        backend: BaseBackend,
        ttl: Optional[int] = DEFAULT_TTL,
        enable_pubsub: Optional[bool] = None,
    ) -> None:
        self.backend = backend
        self.ttl = ttl
        self.enable_pubsub = backend.SUPPORTS_PUBSUB if enable_pubsub is None else enable_pubsub  # fmt: skip

        if enable_pubsub and not backend.SUPPORTS_PUBSUB:
            raise AssertionError("Backend does not support PUBSUB")

    def serialize(self) -> dict[str, Any]:
        return {
            "backend": self.backend.encode(),
            "ttl": self.ttl,
            "enable_pubsub": self.enable_pubsub,
        }

    @staticmethod
    @abstractmethod
    def deserialize(data: dict[str, Any]) -> BaseStatusTracker:
        raise NotImplementedError()

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

    @abstractmethod
    def exists(self, message: Message) -> bool:
        """Check if a status for ``message`` exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            bool

        """
        raise NotImplementedError()

    @abstractmethod
    def publish_update(self, message: Message, update: StatusUpdate) -> None:
        """Publish a PUBSUB update for a message.

        Args:
            message (Message): an Alsek message
            update (StatusUpdate): a status to publish

        Returns:
            None

        """
        raise NotImplementedError()

    @abstractmethod
    def listen_to_updates(
        self,
        message: Message,
        auto_exit: bool = True,
    ) -> Iterable[StatusUpdate] | AsyncIterable[StatusUpdate]:
        """Listen to PUBSUB updates for ``message``.

        Args:
            message (Message): an Alsek message
            auto_exit (bool): if ``True`` stop listening if a terminal status for the
                task is encountered (succeeded or failed).

        Returns:
            stream (Iterable[StatusUpdate] | AsyncIterable[StatusUpdate]): A stream of updates from the pubsub channel

        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def get(self, message: Message) -> StatusUpdate:
        """Get the status of ``message``.

        Args:
            message (Message): an Alsek message

        Returns:
            status (StatusUpdate): the status of ``message``

        """
        raise NotImplementedError()

    @abstractmethod
    def wait_for(
        self,
        message: Message,
        status: TaskStatus | tuple[TaskStatus, ...] | list[TaskStatus],
        timeout: Optional[float] = 5.0,
        poll_interval: float = 0.05,
    ) -> bool:
        """Wait for a message to reach a desired status.

        Args:
            message (Message): the message to monitor
            status (TaskStatus, tuple[TaskStatus...], list[TaskStatus]): the target status
            timeout (float, optional): max time to wait (in seconds). None means wait forever.
            poll_interval (float): how often to check (in seconds)

        Returns:
            bool: True if desired status reached, False if timed out
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()
