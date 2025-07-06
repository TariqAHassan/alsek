"""

    Test Integrity

"""

import pytest

from alsek import Broker, Message
from alsek import StatusTracker
from alsek.core.status.integrity import (
    _name2message,  # noqa
    StatusTrackerIntegrityScanner,
)
from alsek.core.status.types import TaskStatus


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
    integry_scanner = StatusTrackerIntegrityScanner(
        status_tracker=rolling_status_tracker,
        broker=broker,
    )

    # Run a scan
    integry_scanner.scan()

    # Check that the status of this message is now 'UNKNOWN'.
    assert rolling_status_tracker.get(message).status == TaskStatus.UNKNOWN
