"""

    Test Task Decorator

"""
from functools import partial
from typing import Any, Optional, Type, Union

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek.core.broker import Broker
from alsek.core.task import Task, TriggerTask, _parse_base_task, task

TestTriggerTask = partial(TriggerTask, trigger=IntervalTrigger(days=1))


@pytest.mark.parametrize(
    "base_task,trigger,expected",
    [
        # No base task & no trigger
        (None, None, Task),
        (None, None, Task),
        (None, None, Task),
        # No base task & trigger
        (None, CronTrigger(), TriggerTask),
        (None, DateTrigger(), TriggerTask),
        (None, IntervalTrigger(), TriggerTask),
        # Base task & no trigger
        (Task, None, Task),
        (TriggerTask, None, TriggerTask),
        (Task, None, Task),
        # Bask task & trigger
        (TriggerTask, CronTrigger(), TriggerTask),
        (TriggerTask, DateTrigger(), TriggerTask),
        (TriggerTask, IntervalTrigger(), TriggerTask),
        # Note: (Task, Trigger) is handled in `task()` itself.
        #       See `test_invalid_trigger()` below.
    ],
)
def test_parse_base_task(
    base_task: Optional[Type[Task]],
    trigger: Optional[Union[CronTrigger, DateTrigger, IntervalTrigger]],
    expected: Any,
) -> None:
    assert _parse_base_task(base_task, trigger=trigger) is expected


@pytest.mark.parametrize("base_task", [Task, TestTriggerTask])
def test_task(base_task: Task, rolling_broker: Broker) -> None:
    @task(rolling_broker, base_task=base_task)
    def func() -> None:
        return 99

    assert isinstance(func, Task)


def test_invalid_trigger(rolling_broker: Broker) -> None:
    with pytest.raises(ValueError):

        @task(rolling_broker, base_task=Task, trigger=IntervalTrigger())
        def func() -> None:
            return 99
