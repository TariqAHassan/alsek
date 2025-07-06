"""

    Conftest

"""

from __future__ import annotations

from functools import partial, wraps
from subprocess import PIPE, Popen
from typing import Any, Iterable, Optional, Type, Union

import pytest
from _pytest.fixtures import SubRequest
from click.testing import CliRunner
from pytest_redis import factories as redis_factories
from redis import Redis

from redis.asyncio import Redis as RedisAsync

from alsek.core.status.asyncio import AsyncStatusTracker
from alsek.cli.cli import main as alsek_cli
from alsek.core.broker import Broker
from alsek.core.concurrency import Lock, ProcessLock, ThreadLock
from alsek.core.message import Message
from alsek.core.status.standard import StatusTracker
from alsek.core.task import task
from alsek.core.worker.process import ProcessWorkerPool
from alsek.core.worker.thread import ThreadWorkerPool
from alsek.storage.backends.abstract import Backend, AsyncBackend
from alsek.storage.backends.redis import RedisAsyncBackend
from alsek.storage.backends.redis.standard import RedisBackend
from alsek.storage.result import ResultStore
from alsek.tools.iteration import ResultPool
from alsek.types import Empty


def _get_redis_path() -> str:
    r = Popen(["which", "redis-server"], stdout=PIPE)
    value, error = r.communicate()
    if error:
        raise OSError(error)
    return value.decode().strip("\n")


custom_redisdb_proc = redis_factories.redis_proc(
    executable=_get_redis_path(),
    port=None,
    datadir="/tmp",
)
custom_redisdb = redis_factories.redisdb("custom_redisdb_proc", decode=True)


@pytest.fixture()
def cli_runner() -> CliRunner:
    runner = CliRunner()
    runner.invoke = partial(runner.invoke, cli=alsek_cli)
    return runner


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

        def get(
            self,
            name: str,
            default: Optional[Union[Any, Type[Empty]]] = None,
        ) -> Any:
            raise NotImplementedError()

        def priority_add(self, key: str, unique_id: str, priority: int | float) -> None:
            raise NotImplementedError()

        def priority_get(self, key: str) -> Optional[str]:
            raise NotImplementedError()

        def priority_iter(self, key: str) -> Iterable[str]:
            raise NotImplementedError()

        def priority_remove(self, key: str, unique_id: str) -> None:
            raise NotImplementedError()

        def pub(self, channel: str, value: Any) -> None:
            raise NotImplementedError()

        def sub(self, channel: str) -> Iterable[str | dict[str, Any]]:
            raise NotImplementedError()

        def scan(self, pattern: Optional[str] = None) -> Iterable[str]:
            raise NotImplementedError()

        def exists(self, name: str) -> bool:
            raise NotImplementedError()

        def delete(self, name: str, missing_ok: bool = False) -> None:
            raise NotImplementedError()

    return MockBaseBackend()


@pytest.fixture()
def redis_backend(custom_redisdb: Redis) -> RedisBackend:
    return RedisBackend(custom_redisdb)


@pytest.fixture(params=["redis"])
def rolling_backend(
    request: SubRequest,
    custom_redisdb: Redis,  # noqa
) -> Backend:
    if request.param == "redis":
        return RedisBackend(custom_redisdb)
    else:
        raise ValueError(f"Unknown backend '{request.param}'")


@pytest.fixture(params=["redis"])
def rolling_async_backend(
    request: SubRequest,
    custom_redisdb: Redis,  # noqa
) -> AsyncBackend:
    if request.param == "redis":
        return RedisAsyncBackend(
            RedisAsync(
                host=custom_redisdb.connection_pool.connection_kwargs.get(
                    "host",
                    "localhost",
                ),
                port=custom_redisdb.connection_pool.connection_kwargs.get("port", 6379),
                db=custom_redisdb.connection_pool.connection_kwargs.get("db", 0),
                decode_responses=True,
            )
        )
    else:
        raise ValueError(f"Unknown backend '{request.param}'")


@pytest.fixture(
    params=[
        Lock.__name__,
        ProcessLock.__name__,
        ThreadLock.__name__,
    ]
)
def rolling_lock(
    request: SubRequest,
    rolling_backend: Backend,
) -> Lock:
    if request.param == Lock.__name__:
        return Lock("test_lock", rolling_backend)
    elif request.param == ProcessLock.__name__:
        return ProcessLock("test_lock", rolling_backend)
    elif request.param == ThreadLock.__name__:
        return ThreadLock("test_lock", rolling_backend)
    else:
        raise ValueError(f"Unknown lock '{request.param}'")


@pytest.fixture()
def rolling_broker(rolling_backend: Backend) -> Broker:
    return Broker(rolling_backend)


@pytest.fixture()
def rolling_result_store(rolling_backend: Backend) -> ResultStore:
    return ResultStore(rolling_backend)


@pytest.fixture()
def rolling_result_pool(rolling_result_store: ResultStore):
    return ResultPool(rolling_result_store)


@pytest.fixture()
def rolling_status_tracker(rolling_backend: Backend) -> StatusTracker:
    return StatusTracker(rolling_backend)


@pytest.fixture()
def rolling_status_tracker_async(
    rolling_async_backend: AsyncBackend,
) -> AsyncStatusTracker:
    return AsyncStatusTracker(rolling_async_backend)


@pytest.fixture()
def rolling_thread_worker_pool(rolling_broker):
    n = 3
    tasks = [
        task(rolling_broker, name=f"thread-task-{i}", mechanism="thread")(lambda: i)
        for i in range(n)
    ]

    pool = ThreadWorkerPool(tasks=tasks)
    pool.stream = pool._poll  # noqa; single scan → finite run

    # ---- wrap submit_message so it stops the pool after one hit ----
    original_submit = pool.submit_message

    @wraps(original_submit)
    def submit_and_quit(msg: Message) -> bool:
        submitted = original_submit(msg)
        if submitted:  # first successful enqueue
            pool._can_run = False  # break the outer while-loop
        return submitted

    pool.submit_message = submit_and_quit
    return pool


@pytest.fixture()
def rolling_process_worker_pool(rolling_broker: Broker) -> ProcessWorkerPool:
    """
    A pool that will exit after one successful submit_message, and uses
    ._poll() so run() returns once.
    """
    # create 3 trivial “process” tasks
    tasks = [
        task(
            rolling_broker,
            name=f"proc-task-{i}",
            mechanism="process",
        )(lambda: i)
        for i in range(3)
    ]
    pool = ProcessWorkerPool(tasks=tasks)
    # make stream finite
    pool.stream = pool._poll

    # stop after first submit so run() returns
    original = pool.submit_message

    @wraps(original)
    def submit_and_quit(msg: Message) -> bool:
        ok = original(msg)
        if ok:
            pool._can_run = False
        return ok

    pool.submit_message = submit_and_quit
    return pool
