"""

    Test Async Status

"""

import asyncio
from typing import Any

import pytest

from alsek.core.message import Message
from alsek.core.status.asyncio import AsyncStatusTracker
from alsek.core.status.types import TaskStatus, TERMINAL_TASK_STATUSES
from alsek.exceptions import ValidationError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,status,do_set",
    [
        (Message("task", uuid="async-test-1"), TaskStatus.SUBMITTED, True),
        (Message("task", uuid="async-test-2"), TaskStatus.SUBMITTED, False),
    ],
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
    [
        (Message("task", uuid="async-set-1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="async-set-2"), TaskStatus.RUNNING),
        (Message("task", uuid="async-set-3"), TaskStatus.RETRYING),
        (Message("task", uuid="async-set-4"), TaskStatus.FAILED),
        (Message("task", uuid="async-set-5"), TaskStatus.SUCCEEDED),
    ],
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
    [
        (Message("task", uuid="async-get-1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="async-get-2"), TaskStatus.RUNNING),
        (Message("task", uuid="async-get-3"), TaskStatus.RETRYING),
        (Message("task", uuid="async-get-4"), TaskStatus.FAILED),
        (Message("task", uuid="async-get-5"), TaskStatus.SUCCEEDED),
    ],
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
    [
        (Message("task", uuid="async-del-check-1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="async-del-check-2"), TaskStatus.RUNNING),
        (Message("task", uuid="async-del-check-3"), TaskStatus.RETRYING),
        (Message("task", uuid="async-del-check-4"), TaskStatus.FAILED),
        (Message("task", uuid="async-del-check-5"), TaskStatus.SUCCEEDED),
    ],
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
    [
        (Message("task", uuid="async-del-no-check-1"), TaskStatus.SUBMITTED),
        (Message("task", uuid="async-del-no-check-2"), TaskStatus.RUNNING),
        (Message("task", uuid="async-del-no-check-3"), TaskStatus.RETRYING),
        (Message("task", uuid="async-del-no-check-4"), TaskStatus.FAILED),
        (Message("task", uuid="async-del-no-check-5"), TaskStatus.SUCCEEDED),
    ],
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
    [
        # single-status arg, status will be set → True
        (TaskStatus.SUCCEEDED, TaskStatus.SUCCEEDED, True, True),
        # iterable-status arg, status will be set to a matching terminal → True
        ([TaskStatus.FAILED, TaskStatus.SUCCEEDED], TaskStatus.FAILED, True, True),
        # status never set → timeout → False
        (TaskStatus.SUCCEEDED, None, False, False),
    ],
)
async def test_wait_for_various_cases(
    rolling_status_tracker_async: AsyncStatusTracker,
    status_arg,
    final_status,
    should_set: bool,
    expected: bool,
) -> None:
    msg = Message("task", uuid=f"async-wait-{str(status_arg).replace(' ', '')}-{final_status}-{should_set}")
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
    [
        "not-a-status",
        123,
        object(),
    ],
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
