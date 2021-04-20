"""

    Test Worker Futures

"""
import pytest
from alsek.core.task import task
from alsek.core.broker import Broker
from tests._helpers import sleeper
from alsek._utils.waiting import waiter
from alsek.exceptions import TerminationError
from alsek.core.message import Message
from alsek.core.futures import TaskFuture, ThreadTaskFuture, ProcessTaskFuture
from typing import Type


@pytest.mark.parametrize(
    "mechansim",
    [
        ThreadTaskFuture,
        ProcessTaskFuture,
    ],
)
def test_future(mechansim: Type[TaskFuture], rolling_broker: Broker) -> None:
    testing_task = task(rolling_broker)(lambda: 1).defer()
    testing_msg = testing_task.generate()

    future = mechansim(testing_task, message=testing_msg)
    assert waiter(lambda: future.complete, timeout=5 * 1000)


@pytest.mark.parametrize(
    "mechansim",
    [
        # ThreadTaskFuture,
        ProcessTaskFuture,
    ],
)
def test_future_stop(mechansim: Type[TaskFuture], rolling_broker: Broker) -> None:
    def no_stop():
        while True:
            sleeper(10, buffer=0)

    testing_task = task(rolling_broker)(no_stop)
    testing_msg = testing_task.generate()

    future = mechansim(testing_task, message=testing_msg)

    # Wait for the future to spin up
    sleeper(100)
    # Check that the future is active
    assert not future.complete
    # Stop the future
    future.stop(TerminationError)
    # Validate that the future has stopped ("completed")
    assert future.complete
