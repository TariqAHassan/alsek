"""

    Test Standard Status

"""

import threading
import time
from typing import Any, Type

import pytest  # noqa

from alsek.core.message import Message
from alsek.core.status.standard import StatusTracker
from alsek.core.status.types import TERMINAL_TASK_STATUSES, TaskStatus
from alsek.exceptions import ValidationError

from ._helpers import TestCaseForStatusTrackingGenerator

# Initialize test case generator for sync tests
test_case_generator = TestCaseForStatusTrackingGenerator(is_async=False)


@pytest.mark.parametrize(
    "message,status,do_set",
    test_case_generator.status_exists_test_cases,
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
    test_case_generator.status_set_test_cases,
)
def test_status_set(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker: StatusTracker,
) -> None:
    assert rolling_status_tracker.set(message, status=status) is None


@pytest.mark.parametrize(
    "message,status",
    test_case_generator.status_get_test_cases,
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
    test_case_generator.status_delete_check_test_cases,
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
    test_case_generator.status_delete_no_check_test_cases,
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
    "status_arg, final_status, should_set, expected",
    test_case_generator.wait_for_various_cases_test_cases,
)
def test_wait_for_various_cases(
    rolling_status_tracker: StatusTracker,
    status_arg,
    final_status,
    should_set: bool,
    expected: TaskStatus | Type[Exception],
) -> None:
    msg = Message("task")
    if should_set:
        # delay then set the final_status
        def _delayed_set() -> None:
            time.sleep(0.05)
            rolling_status_tracker.set(msg, status=final_status)  # type: ignore[arg-type]

        threading.Thread(target=_delayed_set, daemon=True).start()

    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            rolling_status_tracker.wait_for(
                message=msg,
                status=status_arg,
                timeout=0.5,
                poll_interval=0.01,
            )
    else:
        result = rolling_status_tracker.wait_for(
            message=msg,
            status=status_arg,
            timeout=0.5,
            poll_interval=0.01,
        )
        assert result == expected


@pytest.mark.parametrize(
    "bad_status",
    test_case_generator.wait_for_invalid_status_test_cases,
)
def test_wait_for_invalid_status_types_raise(
    rolling_status_tracker: StatusTracker,
    bad_status: Any,
) -> None:
    msg = Message("task", uuid="wf-invalid")
    with pytest.raises(ValueError):
        rolling_status_tracker.wait_for(message=msg, status=bad_status)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "status_arg, final_status, should_set, expected",
    test_case_generator.wait_for_no_raise_on_timeout_test_cases,
)
def test_wait_for_no_raise_on_timeout(
    rolling_status_tracker: StatusTracker,
    status_arg,
    final_status,
    should_set: bool,
    expected: TaskStatus,
) -> None:
    msg = Message("task", uuid="wf-no-raise")
    if should_set:
        # delay then set the final_status
        def _delayed_set() -> None:
            time.sleep(0.05)
            rolling_status_tracker.set(msg, status=final_status)  # type: ignore[arg-type]

        threading.Thread(target=_delayed_set, daemon=True).start()

    result = rolling_status_tracker.wait_for(
        message=msg,
        status=status_arg,
        timeout=0.5,
        poll_interval=0.01,
        raise_on_timeout=False,
    )
    assert result == expected


def test_status_tracker_serialize_deserialize(
    rolling_status_tracker: StatusTracker,
) -> None:
    # Serialize
    serialized = rolling_status_tracker.serialize()

    # Deserialize
    rebuilt_tracker = StatusTracker.deserialize(serialized)

    # Sanity check: rebuilt instance is a StatusTracker
    assert isinstance(rebuilt_tracker, StatusTracker)

    # Deep test: use it
    message = Message("task", uuid="test-uuid")
    rebuilt_tracker.set(message, status=TaskStatus.SUCCEEDED)
    status = rebuilt_tracker.get(message).status
    assert status == TaskStatus.SUCCEEDED
