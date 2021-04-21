"""

    Conftest

"""
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Any, Iterable, Optional

import pytest
from _pytest.fixtures import SubRequest
from pytest_redis import factories as redis_factories
from redis import Redis

from alsek.core.broker import Broker
from alsek.core.task import task
from alsek.core.worker import WorkerPool
from alsek.storage.backends import Backend
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.backends.redis import RedisBackend
from alsek.storage.result import ResultStore


def _get_redis_path() -> str:
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


@pytest.fixture(params=["redis", "diskcache"])
def rolling_backend(
    request: SubRequest,
    tmp_path: Path,
    custom_redisdb: Redis,
) -> Backend:
    if request.param == "redis":
        return RedisBackend(custom_redisdb)
    elif request.param == "diskcache":
        return DiskCacheBackend(tmp_path)
    else:
        raise ValueError(f"Unknown backend '{request.param}'")


@pytest.fixture()
def rolling_broker(rolling_backend: Backend) -> Broker:
    return Broker(rolling_backend)


@pytest.fixture()
def rolling_result_store(rolling_backend: Backend) -> ResultStore:
    return ResultStore(rolling_backend)


@pytest.fixture()
def rolling_worker_pool(rolling_broker: Broker) -> WorkerPool:
    n: int = 3
    tasks = [task(rolling_broker, name=f"task-{i}")(lambda: i) for i in range(n)]
    pool = WorkerPool(tasks)

    # Replace `stream()` with `_poll()`. This means that calls of
    # `run()` wil now exit, rather than looping indefinitely.
    pool.stream = pool._poll
    return pool
