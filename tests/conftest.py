"""

    Conftest

"""
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Any, Iterable, Optional

import pytest
from pytest_redis import factories as redis_factories
from redis import Redis

from alsek.storage.backends import Backend
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.backends.redis import RedisBackend
from alsek.storage.result import ResultStore


def _get_redis_path(command: str = "which redis-server") -> str:
    r = Popen(["which", "redis-server"], stdout=PIPE)
    value, error = r.communicate()
    if error:
        raise OSError(error)
    return value.decode().strip("\n")


custom_redisdb_proc = redis_factories.redis_proc(
    executable=_get_redis_path(),
    port=None,
    logsdir="/tmp",
)
custom_redisdb = redis_factories.redisdb("custom_redisdb_proc", decode=True)


@pytest.fixture()
def base_backend() -> Backend:
    class MockBaseBackend(Backend):
        def set(
            self,
            name: str,
            value: Any,
            nx: bool = False,
            ttl: Optional[int] = None,
        ) -> None:
            raise NotImplementedError()

        def get(self, name: str) -> Any:
            raise NotImplementedError()

        def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
            raise NotImplementedError()

        def exists(self, name: str) -> bool:
            raise NotImplementedError()

        def delete(self, name: str, missing_ok: bool = False) -> None:
            raise NotImplementedError

    return MockBaseBackend()


@pytest.fixture()
def redis_backend(custom_redisdb: Redis) -> RedisBackend:
    return RedisBackend(custom_redisdb)


@pytest.fixture()
def disk_cache_backend(tmp_path: Path) -> DiskCacheBackend:
    return DiskCacheBackend(tmp_path)


@pytest.fixture(params=["redis", "diskcache"])
def rolling_backend(request, tmp_path: Path, custom_redisdb: Redis) -> Backend:
    if request.param == "redis":
        return RedisBackend(custom_redisdb)
    elif request.param == "diskcache":
        return DiskCacheBackend(tmp_path)
    else:
        raise ValueError(f"Unknwon backend '{request.param}'")


@pytest.fixture(params=["redis", "diskcache"])
def result_store(
    request,
    custom_redisdb: Redis,
    tmp_path: Path,
) -> ResultStore:
    if request.param == "redis":
        backend = RedisBackend(custom_redisdb)
    elif request.param == "diskcache":
        backend = DiskCacheBackend(tmp_path)
    else:
        raise ValueError(f"Unknwon backend '{request.param}'")
    return ResultStore(backend)
