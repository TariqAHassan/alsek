"""

    Test Async Status

"""

import asyncio
from typing import Any

import pytest

from alsek.core.message import Message
from alsek.core.status.asyncio import AsyncStatusTracker
from alsek.core.status.types import TERMINAL_TASK_STATUSES, TaskStatus
from alsek.exceptions import ValidationError

from ._helpers import TestCaseForStatusTrackingGenerator

# Initialize test case generator for async tests
test_case_generator = TestCaseForStatusTrackingGenerator(is_async=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status,do_set",
    test_case_generator.status_exists_test_cases,
)
async def test_status_exists(
    message: Message,
    status: TaskStatus,
    do_set: bool,
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    if do_set:
        await rolling_status_tracker_async.set(message, status=status)
        assert await rolling_status_tracker_async.exists(message)
    else:
        assert not await rolling_status_tracker_async.exists(message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status",
    test_case_generator.status_set_test_cases,
)
async def test_status_set(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    result = await rolling_status_tracker_async.set(message, status=status)
    assert result is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status",
    test_case_generator.status_get_test_cases,
)
async def test_status_get(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    await rolling_status_tracker_async.set(message, status=status)
    value = (await rolling_status_tracker_async.get(message)).status
    assert value == status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status",
    test_case_generator.status_delete_check_test_cases,
)
async def test_status_delete_check(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    await rolling_status_tracker_async.set(message, status=status)
    if status in TERMINAL_TASK_STATUSES:
        await rolling_status_tracker_async.delete(message)
        assert not await rolling_status_tracker_async.exists(message)
    else:
        with pytest.raises(ValidationError):
            await rolling_status_tracker_async.delete(message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status",
    test_case_generator.status_delete_no_check_test_cases,
)
async def test_status_delete_no_check(
    message: Message,
    status: TaskStatus,
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    await rolling_status_tracker_async.set(message, status=status)
    await rolling_status_tracker_async.delete(message, check=False)
    assert not await rolling_status_tracker_async.exists(message)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status_arg, final_status, should_set, expected",
    test_case_generator.wait_for_various_cases_test_cases,
)
async def test_wait_for_various_cases(
    rolling_status_tracker_async: AsyncStatusTracker,
    status_arg,
    final_status,
    should_set: bool,
    expected: bool,
) -> None:
    msg = Message(
        "task",
        uuid=f"async-wait-{str(status_arg).replace(' ', '')}-{final_status}-{should_set}",
    )
    if should_set:
        # delay then set the final_status
        async def _delayed_set() -> None:
            await asyncio.sleep(0.05)
            await rolling_status_tracker_async.set(msg, status=final_status)  # type: ignore[arg-type]

        asyncio.create_task(_delayed_set())

    result = await rolling_status_tracker_async.wait_for(
        message=msg,
        status=status_arg,
        timeout=0.2,
        poll_interval=0.01,
    )
    assert result is expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_status",
    test_case_generator.wait_for_invalid_status_test_cases,
)
async def test_wait_for_invalid_status_types_raise(
    rolling_status_tracker_async: AsyncStatusTracker,
    bad_status: Any,
) -> None:
    msg = Message("task", uuid=f"async-wf-invalid-{str(bad_status).replace(' ', '')}")
    with pytest.raises(ValueError):
        await rolling_status_tracker_async.wait_for(message=msg, status=bad_status)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_status_tracker_serialize_deserialize(
    rolling_status_tracker_async: AsyncStatusTracker,
) -> None:
    # Serialize
    serialized = rolling_status_tracker_async.serialize()

    # Deserialize
    rebuilt_tracker = AsyncStatusTracker.deserialize(serialized)

    # Sanity check: rebuilt instance is an AsyncStatusTracker
    assert isinstance(rebuilt_tracker, AsyncStatusTracker)

    # Deep test: use it
    message = Message("task", uuid="async-test-serialize-uuid")
    await rebuilt_tracker.set(message, status=TaskStatus.SUCCEEDED)
    status = (await rebuilt_tracker.get(message)).status
    assert status == TaskStatus.SUCCEEDED
