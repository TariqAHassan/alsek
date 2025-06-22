"""

    Test Namespacing

"""

from typing import Optional, Union

import pytest

from alsek import Message
from alsek.exceptions import ValidationError
from alsek.utils.namespacing import (
    get_dlq_message_name,
    get_message_name,
    get_messages_namespace,
    get_priority_namespace,
    get_priority_namespace_from_message,
    get_pubsub_channel_name,
    get_result_name,
    get_stable_result_prefix,
    get_status_name,
    get_subnamespace,
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


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "status:queue-a:task-a:uuid-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "status:queue-b:task-b:uuid-b",
        ),
    ],
)
def test_get_status_name(message: Message, expected: str) -> None:
    assert get_status_name(message) == expected


@pytest.mark.parametrize(
    "message",
    [
        # Missing uuid
        Message(task_name="task-a", queue="queue-a", uuid="null"),
        # Missing queue
        Message(task_name="task-a", queue="null", uuid="null"),
        # Missing task_name
        Message(task_name="null", queue="queue-a", uuid="uuid-a"),
    ],
)
def test_get_status_name_validation_error(message: Message) -> None:
    if message.uuid == "null":
        message.uuid = ""
    if message.queue == "null":
        message.queue = ""
    if message.task_name == "null":
        message.task_name = ""

    with pytest.raises(ValidationError):
        get_status_name(message)


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            Message("task-a", queue="queue-a", uuid="uuid-a"),
            "channel:message:uuid-a",
        ),
        (
            Message("task-b", queue="queue-b", uuid="uuid-b"),
            "channel:message:uuid-b",
        ),
        # Test that queue and task_name don't affect the result
        (
            Message("different-task", queue="different-queue", uuid="same-uuid"),
            "channel:message:same-uuid",
        ),
    ],
)
def test_get_pubsub_channel_name(message: Message, expected: str) -> None:
    assert get_pubsub_channel_name(message) == expected


def test_get_pubsub_channel_name_validation_error() -> None:
    # No uuid
    message = Message("task-a", queue="queue-a")
    message.uuid = ""
    with pytest.raises(ValidationError):
        get_pubsub_channel_name(message)


@pytest.mark.parametrize(
    "message,expected",
    [
        (Message("task", uuid="uuid"), "results:uuid"),
        (Message("task", uuid="uuid-1", progenitor_uuid="uuid-0"), "results:uuid-0"),
    ],
)
def test_get_stable_result_prefix(
    message: Message,
    expected: str,
) -> None:
    assert get_stable_result_prefix(message) == expected


@pytest.mark.parametrize(
    "message,expected",
    [
        (Message("task", uuid="uuid"), "results:uuid"),
        (
            Message("task", uuid="uuid-1", progenitor_uuid="uuid-0"),
            "results:uuid-0:descendants:uuid-1",
        ),
    ],
)
def test_get_result_name(
    message: Message,
    expected: str,
) -> None:
    assert get_result_name(message) == expected
