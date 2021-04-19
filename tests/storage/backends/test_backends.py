"""

    Test Redis

"""
import time
from typing import Any, Optional

import pytest

from alsek.storage.backends import Backend
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.backends.redis import RedisBackend
from tests._helpers import expand_params_factory

_ALL_BACKENDS = ("disk_cache_backend", "redis_backend")
_expand_to_all_backends = expand_params_factory(_ALL_BACKENDS)


@pytest.mark.parametrize(
    "name,do_set,backend_to_use",
    _expand_to_all_backends(
        ("a", False),
        ("a", True),
        ("apples", False),
        ("apples", True),
    ),
)
def test_exists(
    name: str,
    do_set: bool,
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    if do_set:
        backend.set(name, value=1)
        assert backend.exists(name)
    else:
        assert not backend.exists(name)


@pytest.mark.parametrize(
    "name,value,nx,ttl,backend_to_use",
    _expand_to_all_backends(
        ("a", 1, True, 10),
        ("b", [1, 2, 3], True, 10),
    ),
)
def test_set(
    name: str,
    value: Any,
    nx: bool,
    ttl: Optional[int],
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    backend.set(name, value=value, nx=nx, ttl=ttl)


@pytest.mark.parametrize("backend_to_use", _ALL_BACKENDS)
def test_set_nx(
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend = locals().get(backend_to_use)

    backend.set("a", value="b")
    with pytest.raises(KeyError):
        backend.set("a", value="b", nx=True)


@pytest.mark.parametrize("backend_to_use", _ALL_BACKENDS)
def test_set_ttl(
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    value = "b"
    backend.set("a", value=value, nx=False, ttl=50)
    time.sleep(0.1)
    actual = backend.get("a")
    assert actual is None


@pytest.mark.parametrize("backend_to_use", _ALL_BACKENDS)
def test_get(
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    value = "b"
    backend.set("a", value=value)
    assert backend.get("a") == value


@pytest.mark.parametrize("backend_to_use", _ALL_BACKENDS)
def test_delete(
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    value = "b"
    backend.set("a", value=value)
    assert backend.get("a") == value
    backend.delete("a")
    assert backend.get("a") == None


@pytest.mark.parametrize("backend_to_use", _ALL_BACKENDS)
def test_scan(
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    backend.set("prefix-a", value=1)
    backend.set("prefix-b", value=1)
    assert (
        {i for i in backend.scan()}
        == {i for i in backend.scan("prefix*")}
        == {"prefix-a", "prefix-b"}
    )


@pytest.mark.parametrize(
    "pattern,expected,backend_to_use",
    _expand_to_all_backends(
        (None, 0),
        (None, 10),
        ("apples", 0),
        ("apples", 10),
    ),
)
def test_count(
    pattern: Optional[str],
    expected: int,
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    for i in range(expected):
        backend.set(f"{pattern}:{i}", value=1)
    assert backend.count(f"{pattern}*" if pattern else None) == expected


@pytest.mark.parametrize(
    "to_add,backend_to_use",
    _expand_to_all_backends(0, 10, 500),
)
def test_clear_namespace(
    to_add: int,
    backend_to_use: str,
    disk_cache_backend: DiskCacheBackend,
    redis_backend: RedisBackend,
) -> None:
    backend: Backend = locals().get(backend_to_use)

    for i in range(to_add):
        backend.set(f"key-{i}", value=1)
    backend.clear_namespace()
    assert backend.count() == 0
