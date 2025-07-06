"""

    Test Redis

"""

import threading
import time
from typing import Any, Callable, Optional, Union

import dill
import pytest  # noqa
from redis import Redis

from alsek.storage.backends.abstract import Backend
from alsek.storage.backends.lazy import LazyClient
from alsek.storage.backends.redis.standard import RedisBackend


class Delayed:
    def __init__(self, to_run: Callable[..., Any]) -> None:
        self.to_run = to_run

    def __call__(self) -> Any:
        return self.to_run()


@pytest.mark.parametrize("backend", [RedisBackend])
def test_invalid_conn(backend: Backend) -> None:
    with pytest.raises(ValueError):
        backend._conn_parse(-1)


@pytest.mark.parametrize(
    "conn",
    # ToDo: add test for strings
    [
        None,
        Delayed(lambda: Redis()),
        LazyClient(lambda: Redis()),
    ],
)
def test_redis_connection_parser(
    conn: Optional[Union[Delayed, LazyClient]],
    redis_backend: RedisBackend,
) -> None:
    if isinstance(conn, Delayed):
        conn = conn()

    expected = LazyClient if isinstance(conn, LazyClient) else Redis
    assert isinstance(RedisBackend._conn_parse(conn), expected)


@pytest.mark.parametrize(
    "name,do_set,",
    [
        ("a", False),
        ("a", True),
        ("apples", False),
        ("apples", True),
    ],
)
def test_exists(name: str, do_set: bool, rolling_backend: Backend) -> None:
    if do_set:
        rolling_backend.set(name, value=1)
        assert rolling_backend.exists(name)
    else:
        assert not rolling_backend.exists(name)


def test_encoding(rolling_backend: Backend) -> None:
    encoded_backend = rolling_backend.encode()
    decoded_backend = rolling_backend._from_settings(
        dill.loads(encoded_backend)["settings"]
    )
    # Verify that the reconstruction yielded the same class type
    assert decoded_backend.__class__ == rolling_backend.__class__


@pytest.mark.parametrize(
    "name,value,nx,ttl",
    [
        ("a", 1, True, 10),
        ("b", [1, 2, 3], True, 10),
    ],
)
def test_set(
    name: str,
    value: Any,
    nx: bool,
    ttl: Optional[int],
    rolling_backend: Backend,
) -> None:
    rolling_backend.set(name, value=value, nx=nx, ttl=ttl)


def test_set_nx(rolling_backend: Backend) -> None:
    rolling_backend.set("a", value="b")
    with pytest.raises(KeyError):
        rolling_backend.set("a", value="b", nx=True)


def test_set_ttl(rolling_backend: Backend) -> None:
    value = "b"
    rolling_backend.set("a", value=value, nx=False, ttl=50)
    time.sleep(0.1)
    actual = rolling_backend.get("a")
    assert actual is None


def test_get(rolling_backend: Backend) -> None:
    value = "b"
    rolling_backend.set("a", value=value)
    assert rolling_backend.get("a") == value


def test_delete(rolling_backend: Backend) -> None:
    value = "b"
    rolling_backend.set("a", value=value)
    assert rolling_backend.get("a") == value
    rolling_backend.delete("a")
    assert rolling_backend.get("a") is None


def test_scan(rolling_backend: Backend) -> None:
    rolling_backend.set("prefix-a", value=1)
    rolling_backend.set("prefix-b", value=1)
    assert (
        {i for i in rolling_backend.scan()}
        == {i for i in rolling_backend.scan("prefix*")}
        == {"prefix-a", "prefix-b"}
    )


@pytest.mark.parametrize(
    "pattern,expected",
    [
        (None, 0),
        (None, 10),
        ("apples", 0),
        ("apples", 10),
    ],
)
def test_count(pattern: Optional[str], expected: int, rolling_backend: Backend) -> None:
    for i in range(expected):
        rolling_backend.set(f"{pattern}:{i}", value=1)
    assert rolling_backend.count(f"{pattern}*" if pattern else None) == expected


@pytest.mark.parametrize(
    "to_add",
    [0, 10, 500],
)
def test_clear_namespace(to_add: int, rolling_backend: Backend) -> None:
    for i in range(to_add):
        rolling_backend.set(f"key-{i}", value=1)
    rolling_backend.clear_namespace()
    assert rolling_backend.count() == 0


def test_priority_add_and_get(rolling_backend: Backend) -> None:
    key = "priority:test"
    id_low = "msg-low"
    id_high = "msg-high"

    rolling_backend.priority_add(key, unique_id=id_low, priority=10)
    rolling_backend.priority_add(key, unique_id=id_high, priority=1)

    # Expect the lowest priority score to be returned
    assert rolling_backend.priority_get(key) == id_high


def test_priority_iter_order(rolling_backend: Backend) -> None:
    key = "priority:ordered"
    items = [("x", 5), ("y", 1), ("z", 3)]

    for uid, score in items:
        rolling_backend.priority_add(key, unique_id=uid, priority=score)

    result = list(rolling_backend.priority_iter(key))
    assert result == ["y", "z", "x"]  # sorted by priority


def test_priority_remove(rolling_backend: Backend) -> None:
    key = "priority:removal"
    uid = "remove-me"

    rolling_backend.priority_add(key, unique_id=uid, priority=99)
    assert rolling_backend.priority_get(key) == uid

    rolling_backend.priority_remove(key, uid)
    assert rolling_backend.priority_get(key) is None


def test_priority_get_empty(rolling_backend: Backend) -> None:
    key = "priority:empty"
    assert rolling_backend.priority_get(key) is None


def test_priority_add_overwrite(rolling_backend: Backend) -> None:
    key = "priority:overwrite"
    uid = "dup"

    rolling_backend.priority_add(key, unique_id=uid, priority=10)
    rolling_backend.priority_add(key, unique_id=uid, priority=1)

    assert rolling_backend.priority_get(key) == uid

    # Should now reflect the new priority order
    items = list(rolling_backend.priority_iter(key))
    assert items == [uid]


def test_pub_sub_simple(rolling_backend: Backend) -> None:
    """Test basic pub/sub functionality with simple string message."""
    if not rolling_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = "test_channel"
    message = "hello world"
    received_messages = []

    def publisher():
        # Small delay to ensure subscriber is ready
        time.sleep(0.1)
        rolling_backend.pub(channel, message)

    # Start publisher in background thread
    publisher_thread = threading.Thread(target=publisher)
    publisher_thread.start()

    # Subscribe and collect messages
    for msg in rolling_backend.sub(channel):
        if isinstance(msg, dict) and msg.get("type") == "message":
            received_messages.append(msg["data"])
            break

    publisher_thread.join(timeout=2.0)

    assert len(received_messages) == 1
    assert received_messages[0] == message


def test_pub_sub_complex_data(rolling_backend: Backend) -> None:
    """Test pub/sub with complex data structures."""
    if not rolling_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = "test-complex"
    complex_message = {
        "id": 12345,
        "data": ["item1", "item2", "item3"],
        "metadata": {"priority": "high", "timestamp": 1234567890},
    }
    received_messages = []

    def publisher():
        time.sleep(0.1)
        rolling_backend.pub(channel, complex_message)

    publisher_thread = threading.Thread(target=publisher)
    publisher_thread.start()

    for msg in rolling_backend.sub(channel):
        if isinstance(msg, dict) and msg.get("type") == "message":
            received_messages.append(msg["data"])
            break

    publisher_thread.join(timeout=2.0)

    assert len(received_messages) == 1
    assert received_messages[0] == complex_message


def test_pub_sub_multiple_messages(rolling_backend: Backend) -> None:
    """Test pub/sub with multiple messages."""
    if not rolling_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = "test-multiple"
    messages = ["message1", "message2", "message3"]
    received_messages = []

    def publisher():
        time.sleep(0.1)
        for msg in messages:
            rolling_backend.pub(channel, msg)
            time.sleep(0.05)  # Small delay between messages

    publisher_thread = threading.Thread(target=publisher)
    publisher_thread.start()

    # Collect messages until we get all expected ones
    for msg in rolling_backend.sub(channel):
        if isinstance(msg, dict) and msg.get("type") == "message":
            received_messages.append(msg["data"])
            if len(received_messages) >= len(messages):
                break

    publisher_thread.join(timeout=3.0)

    assert len(received_messages) == len(messages)
    assert received_messages == messages


@pytest.mark.parametrize(
    "test_data",
    [
        [],
        [42],
        [42, {"key": "value"}],
    ],
)
def test_pub_sub_serialization(test_data: list[Any], rolling_backend: Backend) -> None:
    if not rolling_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = f"test-pubsub-{len(test_data)}"
    received_messages = list()

    def publisher() -> None:
        time.sleep(0.1)
        for i in test_data:
            rolling_backend.pub(channel, i)

    publisher_thread = threading.Thread(target=publisher)
    publisher_thread.start()

    if test_data:
        for payload in rolling_backend.sub(channel):
            if isinstance(payload, dict) and payload.get("type") == "message":
                received_messages.append(payload["data"])
            if len(received_messages) == len(test_data):
                break

    publisher_thread.join(timeout=2.0)
    assert len(received_messages) == len(test_data)
    assert received_messages == test_data
