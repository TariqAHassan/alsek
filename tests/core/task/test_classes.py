"""

    Test Task Classes

"""
import logging
from functools import partial
from typing import Any, Optional, Type

import pytest
from apscheduler.schedulers.base import STATE_PAUSED, STATE_RUNNING, STATE_STOPPED
from apscheduler.triggers.interval import IntervalTrigger
from pytest_mock import MockFixture
from schema import Schema

from alsek.core.broker import Broker
from alsek.core.message import Message
from alsek.core.task import Task, TriggerTask
from alsek.exceptions import SchedulingError, ValidationError
from alsek.storage.result import ResultStore
from tests._helpers import sleeper

TestTriggerTask = partial(TriggerTask, trigger=IntervalTrigger(days=1))


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_task_serialization(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task_data = task_class(lambda: 1, broker=rolling_broker)._serialize()
    Schema({"task": type, "settings": dict}).validate(task_data)
    reconstructed_task = Task._deserialize(task_data)
    assert isinstance(reconstructed_task, Task)


@pytest.mark.parametrize(
    "name,task_class",
    [
        (None, Task),
        (None, TestTriggerTask),
        ("name", Task),
        ("name", TestTriggerTask),
    ],
)
def test_name(
    name: Optional[str],
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    def func() -> None:
        pass

    task = task_class(func, broker=rolling_broker, name=name)
    assert task.name == name if name else "func"


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_repr(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert isinstance(repr(task), str)


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_call(task_class: Type[Task], rolling_broker: Broker) -> None:
    def add() -> int:
        return 2

    task = task_class(add, broker=rolling_broker)
    assert task() == 2


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_defered_task_mode(task_class: Type[Task], rolling_broker: Broker) -> None:
    task = Task(lambda: 1, broker=rolling_broker)
    assert not task.deferred
    task.defer()
    assert task.deferred
    task.cancel_defer()
    assert not task.deferred


def test_task_submit(rolling_broker: Broker) -> None:
    message = Message("task")
    task = Task(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.broker.exists(message)


@pytest.mark.parametrize(
    "with_result_store,task_class",
    [
        (True, Task),
        (True, TestTriggerTask),
        (False, Task),
        (False, TestTriggerTask),
    ],
)
def test_generate_validation(
    with_result_store: bool,
    task_class: Type[Task],
    rolling_backend: Broker,
) -> None:
    task = task_class(
        lambda: 1,
        broker=Broker(rolling_backend),
        result_store=ResultStore(rolling_backend) if with_result_store else None,
    )
    if with_result_store:
        actual = task.generate(result_ttl=1)
        assert isinstance(actual, Message)
    else:
        with pytest.raises(ValidationError):
            task.generate(result_ttl=1)


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_generate(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)

    # Generate an instance of the task
    message = task.generate()

    if isinstance(task, TriggerTask):
        # Check that the task was sent to the scheduler
        assert task.generated
    else:
        # Check that the message was submitted to the backend
        assert task.broker.exists(message)


@pytest.mark.parametrize(
    "defer,task_class",
    [
        (True, Task),
        (False, Task),
        (True, TestTriggerTask),
        (False, TestTriggerTask),
    ],
)
def test_generate_deffered(
    defer: bool,
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    if defer:
        task.defer()
        assert task.deferred

    message = task.generate()

    if defer:
        # Check that the message was not submitted to the backend
        assert not task.broker.exists(message)
        # Check that deferred mode has been canceled
        assert not task.deferred
    elif isinstance(task, TriggerTask):
        # Check that the task was sent to the scheduler
        assert task.generated
    else:
        # Check that the message was submitted to the backend
        assert task.broker.exists(message)


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_pre_op(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert task.pre_op(Message("task")) is None


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_op(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert task.op(Message("task")) == 1


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_post_op(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert task.post_op(Message("task"), result=1) is None


@pytest.mark.parametrize("task_class", [Task, TestTriggerTask])
def test_execute(
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert task.execute(Message("task")) == 1


@pytest.mark.parametrize(
    "message,exception,max_retries,task_class",
    [
        # Limited number of retries, within bounds
        (Message("task", retries=0), BaseException(), 3, Task),
        (Message("task", retries=0), BaseException(), 3, TestTriggerTask),
        # Limited number of retries, at bounds
        (Message("task", retries=3), BaseException(), 3, Task),
        (Message("task", retries=3), BaseException(), 3, TestTriggerTask),
        # Limited number of retries, beyond bounds
        (Message("task", retries=4), BaseException(), 3, Task),
        (Message("task", retries=4), BaseException(), 3, TestTriggerTask),
        # Limitless retries
        (Message("task", retries=0), BaseException(), None, Task),
        (Message("task", retries=0), BaseException(), None, TestTriggerTask),
        (Message("task", retries=100), BaseException(), None, Task),
        (Message("task", retries=100), BaseException(), None, TestTriggerTask),
    ],
)
def test_do_retry(
    message: Message,
    exception: BaseException,
    max_retries: Optional[int],
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker, max_retries=max_retries)
    response = task.do_retry(message, exception=exception)

    if max_retries is None:
        assert response is True
    elif message.retries < max_retries:
        assert response is True
    else:
        assert response is False


@pytest.mark.parametrize(
    "result,task_class",
    [
        (None, Task),
        (None, TestTriggerTask),
        (100, Task),
        (100, TestTriggerTask),
    ],
)
def test_do_callback(
    result: Any,
    task_class: Type[Task],
    rolling_broker: Broker,
) -> None:
    task = task_class(lambda: 1, broker=rolling_broker)
    assert task.do_callback(Message("task"), result=result)


def test_trigger_task(rolling_broker: Broker, mocker: MockFixture) -> None:
    invocations = list()
    # Suppress noise from apscheduler
    logging.getLogger("apscheduler").setLevel(logging.CRITICAL)

    class _TrackedMultiSubmit:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __call__(self) -> None:
            if not invocations:
                invocations.append(1)

    mocker.patch("alsek.core.task._MultiSubmit", _TrackedMultiSubmit)

    # Generate a triggered task
    trigger_task = TriggerTask(
        lambda: 1,
        trigger=IntervalTrigger(seconds=0.05),
        broker=rolling_broker,
    )
    message = trigger_task.generate()
    # Wait for the scheduler to execute submit a task
    sleeper(100)
    # Check that _(Tracked)MultiSubmit was invoked
    assert len(invocations) > 0


def test_trigger_task_with_params(rolling_broker: Broker) -> None:
    def func_with_params(a: int) -> int:
        return a

    with pytest.raises(SchedulingError):
        TestTriggerTask(func_with_params, broker=rolling_broker)


def test_trigger_task_job(rolling_broker: Broker) -> None:
    task = TestTriggerTask(lambda: 1, broker=rolling_broker)
    assert not task._job
    task.generate()
    assert task._job


def test_trigger_task_submit(rolling_broker: Broker) -> None:
    message = Message("task")
    task = TestTriggerTask(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.generated


def test_trigger_task_clear(rolling_broker: Broker) -> None:
    message = Message("task")
    task = TestTriggerTask(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.generated
    task.clear()
    assert not task.generated


def test_trigger_task_pause(rolling_broker: Broker) -> None:
    message = Message("task")
    task = TestTriggerTask(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.scheduler.running
    task.pause()
    assert task.scheduler.state == STATE_PAUSED


def test_trigger_task_resume(rolling_broker: Broker) -> None:
    message = Message("task")
    task = TestTriggerTask(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.scheduler.state == STATE_RUNNING
    task.pause()
    assert task.scheduler.state == STATE_PAUSED
    task.resume()
    assert task.scheduler.state == STATE_RUNNING


def test_trigger_task_shutdown(rolling_broker: Broker) -> None:
    message = Message("task")
    task = TestTriggerTask(lambda: 1, broker=rolling_broker, name="task")
    task._submit(message)
    assert task.scheduler.running
    task.shutdown()
    assert task.scheduler.state == STATE_STOPPED
