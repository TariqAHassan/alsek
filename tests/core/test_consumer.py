"""

    Test Consumer

"""

from typing import Any, Iterable, Optional, Union

import pytest

from alsek.core.broker import Broker
from alsek.core.concurrency import Lock
from alsek.core.consumer import Consumer, Message, _ConsumptionMutex
from alsek.storage.backends import Backend
from alsek.utils.namespacing import (
    get_message_name,
)


def test_consumption_mutex_acquisition(rolling_backend: Backend) -> None:
    message = Message("task")
    with _ConsumptionMutex(message, backend=rolling_backend) as lock:
        assert lock.acquire()
    assert lock.held


def test_consumption_mutex_settings(rolling_backend: Backend) -> None:
    message = Message("task")
    with _ConsumptionMutex(message, backend=rolling_backend) as lock:
        assert lock.name == message.uuid
        assert lock.ttl >= message.timeout


@pytest.mark.parametrize(
    "subset",
    [
        None,
        ["queue-a", "queue-b", "queue-c"],
        {"queue-a": ["task-a", "task-b"]},
    ],
)
@pytest.mark.timeout(10)
def test_poll_queue_and_task(
    subset: Optional[Union[list[str], dict[str, list[str]]]],
    rolling_broker: Broker,
) -> None:
    consumer = Consumer(rolling_broker, subset=subset)

    # Define messages
    # Note: these initial messages are decoys when `subset != None`.
    messages = [Message("task") for _ in range(3)]
    if isinstance(subset, list):
        target_messages = [Message("task", queue=q) for q in subset]
        messages += target_messages
    elif isinstance(subset, dict):
        target_messages = [Message(t, queue=q) for q, v in subset.items() for t in v]
        messages += target_messages
    else:
        target_messages = messages

    # Add all messages to the backend
    for i in messages:
        rolling_broker.submit(i)

    # Scan the subnamespace(s)
    actual = {get_message_name(m) for m in consumer._poll()}

    # Validate that the expected messages were recovered
    expected = {get_message_name(m) for m in target_messages}
    assert actual == expected


@pytest.mark.parametrize("messages_to_add", range(3))
def test_poll(messages_to_add: int, rolling_broker: Broker) -> None:
    consumer = Consumer(rolling_broker)

    messages = [Message("task") for _ in range(messages_to_add)]
    for i in messages:
        rolling_broker.submit(i)

    actual = set()
    for msg in consumer._poll():
        assert Lock(msg.lock_long_name, backend=rolling_broker.backend).held
        actual.add(msg.uuid)

    expected = {m.uuid for m in messages}
    assert actual == expected


@pytest.mark.parametrize("empty_passes", range(3))
def test_empty_passes(empty_passes: int, rolling_broker: Broker) -> None:
    consumer = Consumer(rolling_broker)
    for _ in range(empty_passes):
        for _ in consumer._poll():
            pass
    assert consumer._empty_passes == empty_passes


@pytest.mark.parametrize("messages_to_add", range(1, 3))
@pytest.mark.timeout(5)
def test_stream(messages_to_add: int, rolling_broker: Broker) -> None:
    consumer = Consumer(rolling_broker)

    messages = [Message("task") for _ in range(messages_to_add)]
    for i in messages:
        rolling_broker.submit(i)

    count: int = 0
    # Note: `messages_to_add` must be > 0 s.t. this loop will run
    for _ in consumer.stream():
        count += 1
        if count >= len(messages):
            break


if __name__ == "__main__":
    from alsek.storage.backends.redis import RedisBackend

    messages_to_add = 2
    subset = None
    rolling_broker = Broker(RedisBackend())
    rolling_broker.backend.clear_namespace()
    sorted(rolling_broker.backend.scan("queues:*"))
