"""

    Status Storage

"""
from enum import Enum
from typing import Optional

from alsek.core.message import Message
from alsek.storage.backends import Backend


class TaskStatus(Enum):
    """Alsek task statuses."""

    SUBMITTED = 1
    RUNNING = 2
    RETRYING = 3
    FAILED = 4
    SUCCEEDED = 5


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
        return self.backend.exists(self.get_storage_name(message))

    def set(self, message: Message, status: TaskStatus) -> None:
        self.backend.set(
            self.get_storage_name(message),
            value=status.name,
            ttl=self.ttl if status == TaskStatus.SUBMITTED else None,
        )

    def get(self, message: Message) -> TaskStatus:
        status_name = self.backend.get(self.get_storage_name(message))
        return TaskStatus[status_name]

    def delete(self, message: Message) -> None:
        self.backend.delete(self.get_storage_name(message), missing_ok=False)
