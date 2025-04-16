"""

    Namespacing

"""
from typing import Optional

from alsek import Message


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
        return f"queues:{queue}:tasks:{task_name}"
    elif queue:
        return f"queues:{queue}"
    else:
        return "queues"


def get_message_name(message: Message) -> str:
    """Get the name for ``message`` in the backend.

    Args:
        message (Message): an Alsek message

    Returns:
        name (str): message-specific name

    """
    subnamespace = get_subnamespace(message.queue, message.task_name)
    return f"{subnamespace}:messages:{message.uuid}"


def get_priority_namespace(message: Message) -> str:
    """Get the priority queue name for a message's queue.

    Args:
        message (Message): an Alsek message

    Returns:
        str: the fully qualified priority queue name
    """
    subnamespace = get_subnamespace(message.queue, message.task_name)
    return f"{subnamespace}:priority"


def get_dlq_message_name(message: Message) -> str:
    """Get the name for ``message`` in the backend's dead letter queue (DLQ).

    Args:
        message (Message): an Alsek message

    Returns:
        dlq_name (str): message-specific name in the DLQ

    """
    return f"dtq:{get_message_name(message)}"
