"""

    Namespacing

"""

from typing import Optional

from alsek.core.message import Message
from alsek.exceptions import ValidationError


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


def get_message_signature(message: Message) -> str:
    """Get the signature for ``message`` in the backend.

    Args:
        message (Message): an Alsek message

    Returns:
        signature (str): message-specific signature.

    """
    return f"{get_message_name(message)}:retry:{message.retries}"


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
        namespace (str): the fully-qualified priority queue name

    """
    subnamespace = get_subnamespace(message.queue, message.task_name)
    return get_priority_namespace(subnamespace)


def get_dlq_message_name(message: Message) -> str:
    """Get the name for ``message`` in the backend's dead letter queue (DLQ).

    Args:
        message (Message): an Alsek message

    Returns:
        dlq_name (str): message-specific name in the DLQ

    """
    return f"{DLQ_NAMESPACE_KEY}:{get_message_name(message)}"


def get_stable_result_prefix(message: Message) -> str:
    """Get a prefix that does not change based on
    whether the message has a progenitor.

    Args:
        message (Message): an Alsek message

    Returns:
        prefix (str): a stable prefix for results

    """
    return f"results:{message.progenitor_uuid if message.progenitor_uuid else message.uuid}"


def get_result_name(message: Message) -> str:
    """Get the key for ``message`` in the backend.

    Args:
        message (Message): an Alsek message

    Returns:
        name (str): message-specific name

    """
    if message.uuid is None:
        raise ValueError("Message does not have a uuid")

    if message.progenitor_uuid:
        return f"results:{message.progenitor_uuid}:descendants:{message.uuid}"
    else:
        return f"results:{message.uuid}"


def get_status_name(message: Message) -> str:
    """Get the key for the status information about the message.

    Args:
        message (Message): an Alsek message

    Returns:
        name (string): the key for the status information

    """
    if (
        not message.uuid
        or not message.queue
        or not message.task_name
        or not message.uuid
    ):
        raise ValidationError("Required attributes not set for message")
    return f"status:{message.queue}:{message.task_name}:{message.uuid}"


def get_pubsub_channel_name(message: Message) -> str:
    """Get the channel for status updates about the message.

    Args:
        message (Message): an Alsek message

    Returns:
        name (string): the channel for the status information

    """
    # Note: we don't include the `message.queue` or `message.task_name`
    #   because they're variable and postgres `notify` can only have support channels of length 63.
    #   The number of characters in the name returned here will always be 52 characters.
    #   (16 for the prefix and 36 for the v4 uuid).
    if not message.uuid:
        raise ValidationError("Required attributes not set for message")
    return f"channel:message:{message.uuid}"
