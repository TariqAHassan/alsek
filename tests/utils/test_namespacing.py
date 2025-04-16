"""

    Test Namespacing

"""
import pytest
from typing import Optional, Union

from alsek import Message
from alsek.utils.namespacing import get_subnamespace, get_message_name


@pytest.mark.parametrize(
    "queue,task_name,expected",
    [
        # No Queue & Task
        (None, "task", ValueError),
        # Queue & task
        ("queue", "task", "queues:queue:tasks:task"),
        # Queue only
        ("queue", None, "queues:queue"),
        # No queue & no task
        (None, None, "queues"),
    ],
)
def test_get_subnamespace(
    queue: Optional[str],
    task_name: Optional[str],
    expected: Union[Exception, str],
) -> None:
    if isinstance(expected, str):
        actual = get_subnamespace(queue, task_name=task_name)
        assert actual == expected
    else:
        with pytest.raises(expected):
            get_subnamespace(queue, task_name=task_name)


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "queues:queue-a:tasks:task-a:messages:uuid-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "queues:queue-b:tasks:task-b:messages:uuid-b",
        ),
    ],
)
def test_get_message_name(
    message: Message,
    expected: Union[BaseException, str],
) -> None:
    actual = get_message_name(message)
    assert actual == expected



