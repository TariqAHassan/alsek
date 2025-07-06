"""

    Types

"""

from __future__ import annotations

from enum import Enum
from typing import Any, NamedTuple, Optional


class TaskStatus(Enum):
    """Alsek task statuses."""

    UNKNOWN = 0
    SUBMITTED = 1
    RUNNING = 2
    RETRYING = 3
    FAILED = 4
    SUCCEEDED = 5


TERMINAL_TASK_STATUSES: tuple[TaskStatus, ...] = (
    TaskStatus.FAILED,
    TaskStatus.SUCCEEDED,
)


class StatusUpdate(NamedTuple):
    """Status information."""

    status: TaskStatus
    details: Optional[Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(
            status=self.status.name,
            details=self.details,
        )
