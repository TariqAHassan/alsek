"""

    Test Status

"""
import pytest

from alsek.core.message import Message
from alsek.core.status import TERMINAL_TASK_STATUSES, StatusTracker, TaskStatus
from alsek.exceptions import ValidationError


@pytest.mark.parametrize(
    "message,status,do_set",
    [
        (Message("task", uuid="1"), TaskStatus.SUBMITTED, True),
        (Message("task", uuid="1"), TaskStatus.SUBMITTED, False),
    ],
)
def test_status_exists(
    message: Message,
    status: TaskStatus,
    do_set: bool,
    rolling_status_tracker: StatusTracker,
) -> None:
    if do_set:
        rolling_status_tracker.set(message, status=status)
        assert rolling_status_tracker.exists(message)
    else:
        assert not rolling_status_tracker.exists(message)


@pytest.mark.parametrize(
    "message,status",
    [
        (Message("task", uuid="1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="2"), TaskStatus.RUNNING),
        (Message("task", uuid="3"), TaskStatus.RETRYING),
        (Message("task", uuid="4"), TaskStatus.FAILED),
        (Message("task", uuid="5"), TaskStatus.SUCCEEDED),
    ],
)
def test_status_set(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker: StatusTracker,
) -> None:
    assert rolling_status_tracker.set(message, status=status) is None


@pytest.mark.parametrize(
    "message,status",
    [
        (Message("task", uuid="1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="2"), TaskStatus.RUNNING),
        (Message("task", uuid="3"), TaskStatus.RETRYING),
        (Message("task", uuid="4"), TaskStatus.FAILED),
        (Message("task", uuid="5"), TaskStatus.SUCCEEDED),
    ],
)
def test_status_get(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker: StatusTracker,
) -> None:
    rolling_status_tracker.set(message, status=status)
    value = rolling_status_tracker.get(message)
    assert value == status


@pytest.mark.parametrize(
    "message,status",
    [
        (Message("task", uuid="1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="2"), TaskStatus.RUNNING),
        (Message("task", uuid="3"), TaskStatus.RETRYING),
        (Message("task", uuid="4"), TaskStatus.FAILED),
        (Message("task", uuid="5"), TaskStatus.SUCCEEDED),
    ],
)
def test_status_delete_check(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker: StatusTracker,
) -> None:
    rolling_status_tracker.set(message, status=status)
    if status in TERMINAL_TASK_STATUSES:
        rolling_status_tracker.delete(message)
        assert not rolling_status_tracker.exists(message)
    else:
        with pytest.raises(ValidationError):
            rolling_status_tracker.delete(message)


@pytest.mark.parametrize(
    "message,status",
    [
        (Message("task", uuid="1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="2"), TaskStatus.RUNNING),
        (Message("task", uuid="3"), TaskStatus.RETRYING),
        (Message("task", uuid="4"), TaskStatus.FAILED),
        (Message("task", uuid="5"), TaskStatus.SUCCEEDED),
    ],
)
def test_status_delete_no_check(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker: StatusTracker,
) -> None:
    rolling_status_tracker.set(message, status=status)
    rolling_status_tracker.delete(message, check=False)
    assert not rolling_status_tracker.exists(message)
