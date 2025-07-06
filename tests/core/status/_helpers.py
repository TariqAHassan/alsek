"""

    Helpers

"""

from alsek import Message
from typing import Any
from alsek.core.status.types import TaskStatus


class TestCaseForStatusTrackingGenerator:
    def __init__(self, is_async: bool) -> None:
        self.is_async = is_async

    @property
    def prefix(self) -> str:
        return "async" if self.is_async else "sync"

    @property
    def status_exists_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-test-1"), TaskStatus.SUBMITTED, True),
            (Message("task", uuid=f"{self.prefix}-test-2"), TaskStatus.SUBMITTED, False),
        ]
        # fmt: on

    @property
    def basic_message_status_test_cases(self) -> list[tuple[Any, ...]]:
        """Used for test_status_set, test_status_get, test_status_delete_check"""
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-basic-1"), TaskStatus.SUBMITTED),
            (Message("task", uuid=f"{self.prefix}-basic-2"), TaskStatus.RUNNING),
            (Message("task", uuid=f"{self.prefix}-basic-3"), TaskStatus.RETRYING),
            (Message("task", uuid=f"{self.prefix}-basic-4"), TaskStatus.FAILED),
            (Message("task", uuid=f"{self.prefix}-basic-5"), TaskStatus.SUCCEEDED),
        ]
        # fmt: on

    @property
    def status_set_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-set-1"), TaskStatus.SUBMITTED),
            (Message("task", uuid=f"{self.prefix}-set-2"), TaskStatus.RUNNING),
            (Message("task", uuid=f"{self.prefix}-set-3"), TaskStatus.RETRYING),
            (Message("task", uuid=f"{self.prefix}-set-4"), TaskStatus.FAILED),
            (Message("task", uuid=f"{self.prefix}-set-5"), TaskStatus.SUCCEEDED),
        ]
        # fmt: on

    @property
    def status_get_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-get-1"), TaskStatus.SUBMITTED),
            (Message("task", uuid=f"{self.prefix}-get-2"), TaskStatus.RUNNING),
            (Message("task", uuid=f"{self.prefix}-get-3"), TaskStatus.RETRYING),
            (Message("task", uuid=f"{self.prefix}-get-4"), TaskStatus.FAILED),
            (Message("task", uuid=f"{self.prefix}-get-5"), TaskStatus.SUCCEEDED),
        ]
        # fmt: on

    @property
    def status_delete_check_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-del-check-1"), TaskStatus.SUBMITTED),
            (Message("task", uuid=f"{self.prefix}-del-check-2"), TaskStatus.RUNNING),
            (Message("task", uuid=f"{self.prefix}-del-check-3"), TaskStatus.RETRYING),
            (Message("task", uuid=f"{self.prefix}-del-check-4"), TaskStatus.FAILED),
            (Message("task", uuid=f"{self.prefix}-del-check-5"), TaskStatus.SUCCEEDED),
        ]
        # fmt: on

    @property
    def status_delete_no_check_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            (Message("task", uuid=f"{self.prefix}-del-no-check-1"), TaskStatus.SUBMITTED),
            (Message("task", uuid=f"{self.prefix}-del-no-check-2"), TaskStatus.RUNNING),
            (Message("task", uuid=f"{self.prefix}-del-no-check-3"), TaskStatus.RETRYING),
            (Message("task", uuid=f"{self.prefix}-del-no-check-4"), TaskStatus.FAILED),
            (Message("task", uuid=f"{self.prefix}-del-no-check-5"), TaskStatus.SUCCEEDED),
        ]
        # fmt: on

    @property
    def wait_for_various_cases_test_cases(self) -> list[tuple[Any, ...]]:
        # fmt: off
        return [
            # single-status arg, status will be set → True
            (TaskStatus.SUCCEEDED, TaskStatus.SUCCEEDED, True, True),
            # iterable-status arg, status will be set to a matching terminal → True
            ([TaskStatus.FAILED, TaskStatus.SUCCEEDED], TaskStatus.FAILED, True, True),
            # status never set → timeout → False
            (TaskStatus.SUCCEEDED, None, False, False),
        ]
        # fmt: on

    @property
    def wait_for_invalid_status_test_cases(self) -> list:
        # fmt: off
        return [
            "not-a-status",
            123,
            object(),
        ]
        # fmt: on
