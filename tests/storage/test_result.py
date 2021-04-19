"""

    Test Result

"""
import pytest
from schema import Schema

from alsek.core.message import Message
from alsek.storage.result import ResultStore
from tests._helpers import sleeper


@pytest.mark.parametrize(
    "message,expected",
    [
        (Message("task", uuid="uuid"), "results:uuid"),
        (Message("task", uuid="uuid-1", progenitor="uuid-0"), "results:uuid-0"),
    ],
)
def test_get_stable_prefix(
    message: Message,
    expected: str,
    rolling_result_store: ResultStore,
) -> None:
    assert rolling_result_store._get_stable_prefix(message) == expected


@pytest.mark.parametrize(
    "message,expected",
    [
        (Message("task", uuid="uuid"), "results:uuid"),
        (
            Message("task", uuid="uuid-1", progenitor="uuid-0"),
            "results:uuid-0:descendants:uuid-1",
        ),
    ],
)
def test_get_storage_name(
    message: Message,
    expected: str,
    rolling_result_store: ResultStore,
) -> None:
    assert rolling_result_store.get_storage_name(message) == expected


@pytest.mark.parametrize(
    "storage_name,expected",
    [
        ("results:uuid", "uuid"),
        ("results:uuid-0:descendants:uuid", "uuid"),
    ],
)
def test_extract_uuid(
    storage_name: Message,
    expected: str,
    rolling_result_store: ResultStore,
) -> None:
    assert rolling_result_store._extract_uuid(storage_name) == expected


@pytest.mark.parametrize(
    "do_set,expected",
    [
        (True, True),
        (False, False),
    ],
)
def test_exists(
    do_set: bool,
    expected: bool,
    rolling_result_store: ResultStore,
) -> None:
    message = Message("task")
    if do_set:
        rolling_result_store.set(message, result=1)
    assert rolling_result_store.exists(message) is expected


@pytest.mark.parametrize(
    "message,nx",
    [
        (Message("task"), True),
        (Message("task"), False),
        (Message("task", result_ttl=100), True),
        (Message("task", result_ttl=100), False),
    ],
)
def test_set(
    message: Message,
    nx: bool,
    rolling_result_store: ResultStore,
) -> None:
    rolling_result_store.set(message, result=1, nx=nx)
    if nx:
        with pytest.raises(KeyError):
            rolling_result_store.set(message, result=1, nx=nx)
    if message.result_ttl:
        sleeper(message.result_ttl)
        assert not rolling_result_store.exists(message)


@pytest.mark.parametrize(
    "do_set",
    [True, False],
)
def test_get_standard(
    do_set: bool,
    rolling_result_store: ResultStore,
) -> None:
    message, result = Message("task"), 1
    if do_set:
        rolling_result_store.set(message, result=result)
        assert rolling_result_store.get(message) == result
    else:
        with pytest.raises(KeyError):
            rolling_result_store.get(message)


@pytest.mark.parametrize(
    "do_set,timeout",
    [
        # Test timeout when the message does exist
        (True, 100),
        # Test timeout when the message does not exist
        (False, 100),
    ],
)
def test_get_no_timeout(
    do_set: bool, timeout: int, rolling_result_store: ResultStore
) -> None:
    message, result = Message("task"), 1
    if do_set:
        rolling_result_store.set(message, result=result)
        assert rolling_result_store.get(message, timeout=timeout) == result
    else:
        with pytest.raises(TimeoutError):
            rolling_result_store.get(message, timeout=timeout)


@pytest.mark.parametrize(
    "keep",
    [True, False],
)
def test_get_no_keep(keep: bool, rolling_result_store: ResultStore) -> None:
    message, result = Message("task"), 1
    rolling_result_store.set(message, result=result)
    rolling_result_store.get(message, keep=keep)

    if keep:
        assert rolling_result_store.exists(message)
    else:
        assert not rolling_result_store.exists(message)


def test_get_descendants(rolling_result_store: ResultStore) -> None:
    progenitor = Message("task")
    decendant_messages = [Message("task", progenitor=progenitor.uuid) for _ in range(3)]
    for msg in (progenitor, *decendant_messages):
        rolling_result_store.set(msg, result=1)

    assert rolling_result_store.get(progenitor, descendants=True) == [1] * 4


@pytest.mark.parametrize(
    "with_metadata",
    [True, False],
)
def test_get_with_metadata(
    with_metadata: bool, rolling_result_store: ResultStore
) -> None:
    message, result = Message("task"), 1
    rolling_result_store.set(message, result=result)

    if with_metadata:
        schema = Schema({"result": result, "timestamp": int, "uuid": str})
    else:
        schema = Schema(result)

    schema.validate(rolling_result_store.get(message, with_metadata=with_metadata))


def test_delete(rolling_result_store: ResultStore) -> None:
    message, result = Message("task"), 1
    rolling_result_store.set(message, result=result)

    assert rolling_result_store.exists(message)
    rolling_result_store.delete(message)
    assert not rolling_result_store.exists(message)
