"""

    Test Worker Helpers

"""

from typing import Collection, Optional, Union

import pytest

from alsek._defaults import DEFAULT_QUEUE
from alsek.core.broker import Broker
from alsek.core.task import Task, task
from alsek.core.worker import _derive_consumer_subset, _extract_broker
from alsek.exceptions import MultipleBrokersError, NoTasksFoundError
from alsek.storage.backends import Backend


def _task_factory(name: str, broker: Broker, queue: Optional[str] = None) -> Task:
    return task(broker, queue=queue, name=name)(lambda: 1)


def test_extract_broker_no_tasks(rolling_broker: Broker) -> None:
    with pytest.raises(NoTasksFoundError):
        assert _extract_broker([])


def test_extract_broker_single_broker(rolling_broker: Broker) -> None:
    tasks = [task(rolling_broker)(lambda: 1) for _ in range(3)]
    assert _extract_broker(tasks) == rolling_broker


def test_extract_broker_multi_broker(rolling_backend: Backend) -> None:
    broker_1 = Broker(rolling_backend)
    broker_2 = Broker(rolling_backend)

    tasks = [task(broker_1)(lambda: 1), task(broker_2)(lambda: 1)]
    with pytest.raises(MultipleBrokersError):
        assert _extract_broker(tasks)


@pytest.mark.parametrize(
    "task_names,queues,expected",
    [
        # Tasks & no queues specified
        (["task-1", "task-2"], None, {DEFAULT_QUEUE: ["task-1", "task-2"]}),
        # Tasks & queues specified
        (
            ["task-1", "task-2"],
            ["queue-b", "queue-a"],
            {"queue-a": ["task-2"], "queue-b": ["task-1"]},
        ),
        # Duplicate queues
        (["task-1", "task-2"], ["queue-a", "queue-a"], ValueError),
    ],
)
def test_derive_consumer_subset(
    task_names: Collection[Task],
    queues: Optional[list[str]],
    expected: Union[dict[str, list[str]], BaseException],
    rolling_broker: Broker,
) -> None:
    if queues:
        tasks = [
            _task_factory(n, broker=rolling_broker, queue=q)
            for q, n in zip(queues, task_names)
        ]
    else:
        tasks = [_task_factory(n, broker=rolling_broker) for n in task_names]

    if isinstance(expected, dict):
        actual = _derive_consumer_subset(tasks, queues=queues)
        assert actual == expected
    else:
        with pytest.raises(expected):
            _derive_consumer_subset(tasks, queues=queues)
