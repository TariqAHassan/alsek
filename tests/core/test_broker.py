"""

    Test Broker

"""
from typing import Optional, Union

import pytest

from alsek._defaults import DEFAULT_TTL
from alsek.core.broker import Broker
from alsek.core.concurrency import Lock
from alsek.core.message import Message
from alsek.exceptions import MessageDoesNotExistsError
from tests._helpers import sleeper


def test_repr(rolling_broker: Broker) -> None:
    assert isinstance(repr(rolling_broker), str)


@pytest.mark.parametrize(
    "queue,task_name,expected",
    [
        # No Queue & Task
        (None, "task", ValueError),
        # Queue & task
        ("queue", "task", "queues:queue:tasks:task"),
        # Queue only
        ("queue", None, "queues:queue"),
        # No queue & no task
        (None, None, "queues"),
    ],
)
def test_get_subnamespace(
    queue: Optional[str],
    task_name: Optional[str],
    expected: Union[BaseException, str],
    rolling_broker: Broker,
) -> None:
    if isinstance(expected, str):
        actual = rolling_broker.get_subnamespace(queue, task_name=task_name)
        assert actual == expected
    else:
        with pytest.raises(expected):
            rolling_broker.get_subnamespace(queue, task_name=task_name)


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "queues:queue-a:tasks:task-a:messages:uuid-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "queues:queue-b:tasks:task-b:messages:uuid-b",
        ),
    ],
)
def test_get_message_name(
    message: Message,
    expected: Union[BaseException, str],
    rolling_broker: Broker,
) -> None:
    actual = rolling_broker.get_message_name(message)
    assert actual == expected


@pytest.mark.parametrize(
    "message,do_submit",
    [
        (Message("task"), True),
        (Message("task"), False),
    ],
)
def test_exists(
    message: Message,
    do_submit: bool,
    rolling_broker: Broker,
) -> None:
    if do_submit:
        rolling_broker.submit(message)
    assert rolling_broker.exists(message) is do_submit


@pytest.mark.parametrize(
    "message,ttl",
    [
        (Message("task-a", queue="queue-a", uuid="uuid-a"), DEFAULT_TTL),
        (Message("task-b", queue="queue-b", uuid="uuid-b"), DEFAULT_TTL),
        (Message("task-a", queue="queue-a", uuid="uuid-a"), 250),
        (Message("task-b", queue="queue-b", uuid="uuid-b"), 250),
    ],
)
@pytest.mark.flaky(max_runs=3)
def test_submit(
    message: Message,
    ttl: int,
    rolling_broker: Broker,
) -> None:
    rolling_broker.submit(message, ttl=ttl)

    assert rolling_broker.exists(message)
    if ttl != DEFAULT_TTL:
        sleeper(ttl)
        assert not rolling_broker.exists(message)


@pytest.mark.parametrize("do_submit", [True, False])
def test_retry(
    do_submit: bool,
    rolling_broker: Broker,
) -> None:
    # The `retry()` methods (intentionally) mutates the
    # original object in memory. For this reason we must
    # create a fresh message each time this function runs.
    message = Message("task")

    if do_submit:
        rolling_broker.submit(message)
        rolling_broker.retry(message)
        recovered_message = Message(
            **rolling_broker.backend.get(rolling_broker.get_message_name(message))
        )
        assert recovered_message.retries == 1
    else:
        with pytest.raises(MessageDoesNotExistsError):
            rolling_broker.retry(message)


def test_clear_lock(rolling_broker: Broker) -> None:
    # Acquire the lock
    lock = Lock("lock", backend=rolling_broker.backend)
    lock.acquire()
    assert lock.held

    # Link to a message
    message = Message("task")._link_lock(lock)
    assert message._lock is not None

    # Clear the lock
    rolling_broker._clear_lock(message)

    # Check that the lock is no longer held
    assert not lock.held

    # Check that the lock is no longer linked to the message
    assert message._lock is None


@pytest.mark.parametrize(
    "method",
    [
        "remove",
        # Note: ack is simply a convenience method and
        #       is functionally the same as `remove()`.
        "ack",
    ],
)
def test_removal(method: str, rolling_broker: Broker) -> None:
    lock = Lock("lock", backend=rolling_broker.backend)
    message = Message("task")._link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now remove it
    getattr(rolling_broker, method)(message)

    # Check that the broker removed the message
    assert not rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message._lock is None
    assert not lock.held


def test_nack(rolling_broker: Broker) -> None:
    lock = Lock("lock", backend=rolling_broker.backend)
    message = Message("task")._link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now nack it
    rolling_broker.nack(message)

    # Check that the broker did not removed the message
    assert rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message._lock is None
    assert not lock.held


@pytest.mark.parametrize("dlq_ttl", [None, 500])
@pytest.mark.flaky(max_runs=3)
def test_fail(dlq_ttl: Optional[int], rolling_broker: Broker) -> None:
    lock = Lock("lock", backend=rolling_broker.backend)
    message = Message("task")._link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now fail it
    rolling_broker.dlq_ttl = dlq_ttl
    rolling_broker.fail(message)

    # Check that the broker removed the message
    assert not rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message._lock is None
    assert not lock.held

    if dlq_ttl:
        # Check that the message has been moved to the dql
        dql_name = f"dlq:{rolling_broker.get_message_name(message)}"
        assert rolling_broker.backend.exists(dql_name)
        # Check that the DQL TTL was respected.
        sleeper(dlq_ttl)
        assert not rolling_broker.backend.exists(dql_name)
