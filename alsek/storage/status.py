"""

    Status Storage

"""
from enum import Enum
from typing import Optional

from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.storage.backends import Backend


class TaskStatus(Enum):
    """Alsek task statuses."""

    SUBMITTED = 1
    RUNNING = 2
    RETRYING = 3
    FAILED = 4
    SUCCEEDED = 5


TERMINAL_TASK_STAUSES = (TaskStatus.FAILED, TaskStatus.SUCCEEDED)


class StatusStore:
    """Alsek Status Store.

    Args:
        backend (Backend): backend for data storage
        ttl (int, optional): time to live (in milliseconds) for the status

    """

    def __init__(
        self,
        backend: Backend,
        ttl: Optional[int] = 60 * 60 * 24 * 7 * 1000,
    ) -> None:
        self.backend = backend
        self.ttl = ttl

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
        return self.backend.exists(self.get_storage_name(message))

    def set(self, message: Message, status: TaskStatus) -> None:
        """Set a ``status`` for ``message``.

        Args:
            message (Message): an Alsek message
            status (TaskStatus): a status to set

        Returns:
            None

        """
        self.backend.set(
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
        status_name = self.backend.get(self.get_storage_name(message))
        return TaskStatus[status_name]

    def delete(self, message: Message, check: bool = True) -> None:
        """Delete the status of ``message``.

        Args:
            message (Message): an Alsek message
            check (bool): check that it is safe to delete the status.
                This is done by ensuring that the current status of ``message``
                is a terminal state (i.e., ``TaskStatus.FAILED`` or
                ``TaskStatus.SUCCEEDED``).

        Returns:
            None

        """
        if check and self.get(message) not in TERMINAL_TASK_STAUSES:
            raise ValidationError(f"Message '{message.uuid}' in a non-terminal state.")
        self.backend.delete(self.get_storage_name(message), missing_ok=False)
