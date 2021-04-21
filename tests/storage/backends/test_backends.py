"""

    Test Redis

"""
import time
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Callable, Optional, Union

import dill
import pytest
from diskcache import Cache as DiskCache
from redis import Redis

from alsek.storage.backends import Backend, LazyClient
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.backends.redis import RedisBackend


class Delayed:
    def __init__(self, to_run: Callable[..., Any]) -> None:
        self.to_run = to_run

    def __call__(self) -> Any:
        return self.to_run()


@pytest.mark.parametrize(
    "conn",
    [
        None,
        Delayed(lambda: mkdtemp()),
        Delayed(lambda: Path(mkdtemp())),
        Delayed(lambda: DiskCache()),
        LazyClient(lambda: DiskCache()),
    ],
)
def test_disk_cache_conn_parse(
    conn: Optional[Union[Delayed, LazyClient]],
    disk_cache_backend: RedisBackend,
) -> None:
    if isinstance(conn, Delayed):
        conn = conn()
    expected = LazyClient if isinstance(conn, LazyClient) else DiskCache
    assert isinstance(DiskCacheBackend._conn_parse(conn), expected)


@pytest.mark.parametrize("backend", [DiskCacheBackend, RedisBackend])
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
def test_redis_conn_parse(
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
    encoded_backend = rolling_backend._encode()
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
