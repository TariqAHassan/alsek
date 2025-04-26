"""

    Test Status

"""

import pytest

from alsek import Broker
from alsek.core.message import Message
from alsek.core.status import (
    TERMINAL_TASK_STATUSES,
    StatusTracker,
    TaskStatus,
    _name2message,
    StatusTrackerIntegryScanner,
)
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
    value = rolling_status_tracker.get(message).status
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


@pytest.mark.parametrize(
    "name,expected",
    [
        ("status:queue1:task1:uuid1", ("task1", "queue1", "uuid1")),
        ("namespace:status:queue2:task2:uuid2", ("task2", "queue2", "uuid2")),
    ],
)
def test_name2message(name, expected: tuple[str, str, str]) -> None:
    message = _name2message(name)
    actual = (message.task_name, message.queue, message.uuid)
    assert actual == expected


def test_integrity_scaner(rolling_status_tracker: StatusTracker) -> None:
    # Simulate a message expiring from the broker by setting the
    # status for a message that has never actually been added to the broker.
    message = Message("task1")
    rolling_status_tracker.set(message, status=TaskStatus.RUNNING)
    broker = Broker(rolling_status_tracker.backend)
    integry_scanner = StatusTrackerIntegryScanner(
        status_tracker=rolling_status_tracker,
        broker=broker,
    )

    # Run a scan
    integry_scanner.scan()

    # Check that the status of this message is now 'UNKNOWN'.
    assert rolling_status_tracker.get(message).status == TaskStatus.UNKNOWN
