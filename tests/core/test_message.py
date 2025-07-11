"""

    Test Message

"""

import copy
import time
from typing import Any, Optional

import pytest

from alsek.core.backoff import ConstantBackoff
from alsek.core.concurrency import Lock
from alsek.core.message import LinkedLock, Message
from alsek.storage.backends.abstract import Backend
from alsek.utils.parsing import ExceptionDetails
from alsek.utils.temporal import utcnow_timestamp_ms


@pytest.mark.parametrize(
    "message",
    [
        Message("task"),
        Message("task", queue="new_queue"),
        Message("task", args=(1, 2, 3)),
        Message("task", kwargs={"a": 1}),
        Message("task", metadata={"meta": "data"}),
        Message("task", exception_details=ExceptionDetails(name=ValueError.__name__)),
        Message("task", result_ttl=1000),
        Message("task", uuid="uuid"),
        Message("task", progenitor_uuid="uuid-0"),
        Message("task", retries=10),
        Message("task", timeout=30),
        Message("task", created_at=0, updated_at=1),
        Message("task", delay=1000),
        Message("task", previous_result=99),
        Message("task", previous_message_uuid="uuid-0"),
        Message("task", callback_message_data=Message("task").data),
        Message("task", backoff_settings=ConstantBackoff().settings),
        Message("task", mechanism="thread"),
        Message("task", linked_lock=LinkedLock(name="lock", owner_id="uuid-0")),
    ],
)
def test_data(message: Message) -> None:
    # Validate the core message can be reconstructed from the data property
    assert Message(**message.data).data == message.data


def test_repr() -> None:
    msg = Message("task")
    repr_string = repr(msg)
    assert all(k in repr_string for k in msg.data)


def test_summary() -> None:
    repr_string = repr(Message("task"))
    assert all(k in repr_string for k in ("uuid", "queue", "task"))


@pytest.mark.parametrize(
    "message,has_backoff",
    [
        (Message("task"), False),
        (Message("task", retries=0), False),
        (Message("task", retries=1), True),
        (Message("task", retries=2), True),
        (Message("task", retries=100), True),
    ],
)
def test_get_backoff_duration(message: Message, has_backoff: bool) -> None:
    actual = message.get_backoff_duration()
    assert actual > 0 if has_backoff else actual == 0


@pytest.mark.parametrize(
    "message,ready_now",
    [
        (Message("task"), True),
        (Message("task", retries=0), True),
        (Message("task", retries=50), False),
        (Message("task", retries=100), False),
    ],
)
def test_ready_at(message: Message, ready_now: bool) -> None:
    now = utcnow_timestamp_ms()
    ready_at = message.ready_at
    if ready_now:
        assert ready_at <= now
    else:
        assert ready_at > now


@pytest.mark.parametrize(
    "message,ready_now",
    [
        (Message("task"), True),
        (Message("task", retries=0), True),
        (Message("task", retries=50), False),
        (Message("task", retries=100), False),
    ],
)
def test_ready(message: Message, ready_now: bool) -> None:
    now = utcnow_timestamp_ms()
    assert message.ready is ready_now


@pytest.mark.parametrize(
    "message,ready_now",
    [
        (Message("task"), True),
        (Message("task", retries=0), True),
        (Message("task", retries=50), False),
        (Message("task", retries=100), False),
    ],
)
def test_ttr(message: Message, ready_now: bool) -> None:
    now = utcnow_timestamp_ms()
    ttr = message.ttr
    if ready_now:
        assert ttr == 0
    else:
        assert ttr > 0


def test_link_lock(rolling_backend: Backend) -> None:
    lock = Lock("lock", rolling_backend)
    msg = Message("task").link_lock(lock)

    assert msg.linked_lock
    assert msg.linked_lock["name"] == lock.name
    assert msg.linked_lock["owner_id"] == lock.owner_id


def test_release_lock(rolling_backend: Backend) -> None:
    lock = Lock("lock", rolling_backend)
    lock.acquire()
    msg = Message("task").link_lock(lock)

    assert msg.linked_lock
    assert msg.linked_lock["name"] == lock.name
    assert msg.linked_lock["owner_id"] == lock.owner_id
    msg.release_lock(not_linked_ok=False, target_backend=rolling_backend)
    assert msg.linked_lock is None


def test_clone() -> None:
    msg = Message("task")
    assert msg.data == msg.clone().data


@pytest.mark.parametrize("data", [{"uuid": "uuid_new", "retries": 99}])
def test_ok_update(data: dict[str, Any]) -> None:
    msg = Message("task")
    msg.update(**data)
    for k, v in data.items():
        assert msg.data[k] == v


@pytest.mark.parametrize("data", [{"apples": 1}, {"oranges": 99}])
def test_bad_update(data: dict[str, Any]) -> None:
    msg = Message("task")
    with pytest.raises(KeyError):
        msg.update(**data)


@pytest.mark.parametrize("new_uuid", [None, "new_uuid"])
def test_duplicate(new_uuid: Optional[str]) -> None:
    msg = Message("task")
    assert msg.uuid != msg.duplicate(new_uuid).uuid


def test_increment_retries() -> None:
    msg = Message("task")
    original_retries = copy.deepcopy(msg.retries)
    original_updated_at = copy.deepcopy(msg.updated_at)

    time.sleep(0.1)
    msg.increment_retries()
    assert msg.retries == 1
    assert msg.updated_at > original_updated_at
