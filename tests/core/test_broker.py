"""

    Test Broker

"""

from typing import Optional

import pytest

from alsek.core.broker import Broker
from alsek.core.concurrency import Lock
from alsek.core.message import Message
from alsek.defaults import DEFAULT_TTL
from alsek.exceptions import MessageDoesNotExistsError
from alsek.utils.namespacing import get_message_name
from tests._helpers import sleeper


def test_repr(rolling_broker: Broker) -> None:
    assert isinstance(repr(rolling_broker), str)


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
            **rolling_broker.backend.get(get_message_name(message))
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
    message = Message("task").link_lock(lock)
    assert message.lock_long_name is not None

    # Clear the lock
    rolling_broker._clear_lock(message)

    # Check that the lock is no longer held
    assert not lock.held

    # Check that the lock is no longer linked to the message
    assert message.lock_long_name is None


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
    message = Message("task").link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now remove it
    getattr(rolling_broker, method)(message)

    # Check that the broker removed the message
    assert not rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message.lock_long_name is None
    assert not lock.held


def test_nack(rolling_broker: Broker) -> None:
    lock = Lock("lock", backend=rolling_broker.backend)
    message = Message("task").link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now nack it
    rolling_broker.nack(message)

    # Check that the broker did not remove the message
    assert rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message.lock_long_name is None
    assert not lock.held


@pytest.mark.parametrize(
    "dlq_ttl,idempotent_check",
    [
        (None, True),
        (None, False),
        (500, True),
        (500, False),
    ],
)
@pytest.mark.flaky(max_runs=3)
def test_fail(
    dlq_ttl: Optional[int],
    idempotent_check: bool,
    rolling_broker: Broker,
) -> None:
    lock = Lock("lock", backend=rolling_broker.backend)
    message = Message("task").link_lock(lock)

    # Add the message via the broker
    rolling_broker.submit(message)
    assert rolling_broker.exists(message)

    # Now fail it
    rolling_broker.dlq_ttl = dlq_ttl
    rolling_broker.fail(message)
    if idempotent_check:
        rolling_broker.fail(message)

    # Check that the broker removed the message
    assert not rolling_broker.exists(message)

    # Check that the lock has been fully released.
    assert message.lock_long_name is None
    assert not lock.held

    if dlq_ttl:
        # Check that the message has been moved to the dtq
        assert rolling_broker.in_dlq(message)
        # Check that the DTQ TTL was respected.
        sleeper(dlq_ttl)
        assert not rolling_broker.in_dlq(message)


@pytest.mark.parametrize("fail", [False, True])
def test_sync(fail: bool, rolling_broker: Broker) -> None:
    message = Message("test")
    rolling_broker.submit(message)
    if fail:
        rolling_broker.fail(message)

    actual = rolling_broker.sync(message)
    assert isinstance(actual, Message)
