"""

    Test Namespacing

"""

import pytest
from typing import Optional, Union

from alsek import Message
from alsek.utils.namespacing import (
    get_messages_namespace,
    get_priority_namespace,
    get_priority_namespace_from_message,
    get_dlq_message_name,
    get_message_name, get_subnamespace,
)


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
        with pytest.raises(expected):  # noqa
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


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-x", queue="queue-x", uuid="uuid-x"),
            "queues:queue-x:tasks:task-x:messages",
        ),
        (
            Message("task-y", queue="queue-y", uuid="uuid-y"),
            "queues:queue-y:tasks:task-y:messages",
        ),
    ],
)
def test_get_messages_namespace(message: Message, expected: str) -> None:
    assert get_messages_namespace(message) == expected


@pytest.mark.parametrize(
    "subnamespace,expected",
    [
        ("queues:q1:tasks:t1", "priority:queues:q1:tasks:t1"),
        ("queues:q2", "priority:queues:q2"),
    ],
)
def test_get_priority_namespace(subnamespace: str, expected: str) -> None:
    assert get_priority_namespace(subnamespace) == expected


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "priority:queues:queue-a:tasks:task-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "priority:queues:queue-b:tasks:task-b",
        ),
    ],
)
def test_get_priority_namespace_from_message(message: Message, expected: str) -> None:
    assert get_priority_namespace_from_message(message) == expected


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "dlq:queues:queue-a:tasks:task-a:messages:uuid-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "dlq:queues:queue-b:tasks:task-b:messages:uuid-b",
        ),
    ],
)
def test_get_dlq_message_name(message: Message, expected: str) -> None:
    assert get_dlq_message_name(message) == expected
