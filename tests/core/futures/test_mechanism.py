"""

    Test Worker Futures

"""

from typing import Type

import pytest

from alsek.core.broker import Broker
from alsek.core.futures import ProcessTaskFuture, TaskFuture, ThreadTaskFuture
from alsek.core.task import task
from alsek.exceptions import TerminationError
from alsek.utils.waiting import waiter
from tests._helpers import sleeper


@pytest.mark.parametrize("mechanism", [ThreadTaskFuture, ProcessTaskFuture])
def test_future(mechanism: Type[TaskFuture], rolling_broker: Broker) -> None:
    testing_task = task(rolling_broker)(lambda: 1).defer()
    testing_msg = testing_task.generate()

    future = mechanism(testing_task, message=testing_msg)
    assert waiter(lambda: future.complete, timeout=5 * 1000)


@pytest.mark.parametrize("mechanism", [ThreadTaskFuture, ProcessTaskFuture])
def test_future_stop(mechanism: Type[TaskFuture], rolling_broker: Broker) -> None:
    def no_stop():
        while True:
            sleeper(10, buffer=0)

    testing_task = task(rolling_broker)(no_stop)
    testing_msg = testing_task.generate()

    future = mechanism(testing_task, message=testing_msg)

    # Check that the future is active
    sleeper(100)
    assert not future.complete
    # Stop the future
    future.stop(TerminationError)
    sleeper(100)
    # Validate that the future has stopped ("completed")
    assert future.complete
    assert isinstance(
        rolling_broker.sync_from_backend(testing_msg).exception_details.as_exception(),
        TerminationError,
    )
