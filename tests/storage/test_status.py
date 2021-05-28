"""

    Test Status

"""
import pytest

from alsek.core.message import Message
from alsek.storage.status import StatusStore, TaskStatus, TERMINAL_TASK_STAUSES
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
    rolling_status_store: StatusStore,
) -> None:
    if do_set:
        rolling_status_store.set(message, status=status)
        assert rolling_status_store.exists(message)
    else:
        assert not rolling_status_store.exists(message)


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
    rolling_status_store: StatusStore,
) -> None:
    value = rolling_status_store.set(message, status=status)
    assert value is None


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
    rolling_status_store: StatusStore,
) -> None:
    rolling_status_store.set(message, status=status)
    value = rolling_status_store.get(message)
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
def test_status_delete(
    message: Message,
    status: TaskStatus,
    rolling_status_store: StatusStore,
) -> None:
    rolling_status_store.set(message, status=status)
    if status in TERMINAL_TASK_STAUSES:
        value = rolling_status_store.delete(message)
        assert not rolling_status_store.exists(message)
    else:
        with pytest.raises(ValidationError):
            rolling_status_store.delete(message)
