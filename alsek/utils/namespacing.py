"""

    Namespacing

"""
from typing import Optional

from alsek.core.message import Message

QUEUES_NAMESPACE_KEY: str = "queues"
TASK_NAMESPACE_KEY: str = "tasks"
MESSAGES_NAMESPACE_KEY: str = "messages"
PRIORITY_NAMESPACE_KEY: str = "priority"
DLQ_NAMESPACE_KEY: str = "dlq"


def get_subnamespace(
    queue: Optional[str] = None,
    task_name: Optional[str] = None,
) -> str:
    """Get the subnamespace for a given ``queue``
    and (optionally) ``task_name``.

    Args:
        queue (str, optional): the name of the queue
        task_name (str): name of the task

    Returns:
        subnamespace (str): queue-specific namespace

    Raises:
        ValueError: if ``task_name`` is provided and ``queue`` is not.

    """
    if queue is None and task_name is not None:
        raise ValueError("`queue` must be provided if `task_name` is not None")

    if queue and task_name:
        return f"{QUEUES_NAMESPACE_KEY}:{queue}:{TASK_NAMESPACE_KEY}:{task_name}"
    elif queue:
        return f"{QUEUES_NAMESPACE_KEY}:{queue}"
    else:
        return f"{QUEUES_NAMESPACE_KEY}"


def get_messages_namespace(message: Message) -> str:
    """Get the namespace for a message.

    Args:
        message (Message): an Alsek message

    Returns:
        namespace (str): the namespace for the message

    """
    subnamespace = get_subnamespace(message.queue, message.task_name)
    return f"{subnamespace}:{MESSAGES_NAMESPACE_KEY}"


def get_message_name(message: Message) -> str:
    """Get the name for ``message`` in the backend.

    Args:
        message (Message): an Alsek message

    Returns:
        name (str): message-specific name

    """
    subnamespace = get_messages_namespace(message)
    return f"{subnamespace}:{message.uuid}"


def get_priority_namespace(subnamespace: str) -> str:
    """Get the namespace for a message's priority information.

    Args:
        subnamespace (str): the namespace for the message

    Returns:
        priority_namespace (str): the namespace for priority information

    """
    return f"{PRIORITY_NAMESPACE_KEY}:{subnamespace}"


def get_priority_namespace_from_message(message: Message) -> str:
    """Get the namespace for message's priority information.

    Args:
        message (Message): an Alsek message

    Returns:
        str: the fully qualified priority queue name

    """
    subnamespace = get_subnamespace(message.queue)
    return f"{get_priority_namespace(subnamespace)}:{TASK_NAMESPACE_KEY}:{message.task_name}"


def get_dlq_message_name(message: Message) -> str:
    """Get the name for ``message`` in the backend's dead letter queue (DLQ).

    Args:
        message (Message): an Alsek message

    Returns:
        dlq_name (str): message-specific name in the DLQ

    """
    return f"{DLQ_NAMESPACE_KEY}:{get_message_name(message)}"
