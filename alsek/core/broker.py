"""

    Broker

"""

import logging
from typing import Optional

from alsek._defaults import DEFAULT_TTL
from alsek.core.concurrency import Lock
from alsek.core.message import Message
from alsek.exceptions import MessageAlreadyExistsError, MessageDoesNotExistsError
from alsek.storage.backends import Backend
from alsek.types import Empty
from alsek.utils.logging import magic_logger
from alsek.utils.printing import auto_repr

log = logging.getLogger(__name__)


class Broker:
    """Alsek Broker.

    Args:
        backend (Backend): backend for data storage
        dlq_ttl (int, optional): time to live (in milliseconds) for
            Dead Letter Queue (DLQ). If ``None``, failed messages
            will not be moved to the DLQ.

    """

    def __init__(self, backend: Backend, dlq_ttl: Optional[int] = DEFAULT_TTL) -> None:
        self.backend = backend
        self.dlq_ttl = dlq_ttl

        if self.backend.IS_ASYNC:
            raise AttributeError("Asynchronous backends are not yet supported")

    def __repr__(self) -> str:
        return auto_repr(
            self,
            backend=self.backend,
            dlq_ttl=self.dlq_ttl,
        )

    @staticmethod
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

    def get_message_name(self, message: Message) -> str:
        """Get the name for ``message`` in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            name (str): message-specific name

        """
        subnamespace = self.get_subnamespace(message.queue, message.task_name)
        return f"{subnamespace}:messages:{message.uuid}"

    def get_priority_name(self, message: Message) -> str:
        """Get the priority queue name for a message's queue.

        Args:
            message (Message): an Alsek message

        Returns:
            str: the fully qualified priority queue name
        """
        subnamespace = self.get_subnamespace(message.queue, message.task_name)
        return f"{subnamespace}:priority"

    def exists(self, message: Message) -> bool:
        """Determine if the message exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            exists (bool): whether the message exists.

        """
        name = self.get_message_name(message)
        return self.backend.exists(name)

    @magic_logger(
        before=lambda message: log.debug("Submitting %s...", message.summary),
        after=lambda input_: log.debug("Submitted %s.", input_["message"].summary),
    )
    def submit(self, message: Message, ttl: int = DEFAULT_TTL) -> None:
        """Submit a message for processing.

        Args:
            message (Message): an Alsek message
            ttl (int): time to live for the submitted message in milliseconds

        Returns:
            None

        Raises:
            MessageAlreadyExistsError: if the message already exists

        """
        name = self.get_message_name(message)
        try:
            self.backend.set(name, value=message.data, nx=True, ttl=ttl)
        except KeyError:
            raise MessageAlreadyExistsError(f"'{name}' found in backend")

        self.backend.priority_add(
            self.get_priority_name(message),
            member=message.uuid,
            priority=message.priority,
        )

    @magic_logger(
        before=lambda message: log.debug("Retrying %s...", message.summary),
    )
    def retry(self, message: Message) -> None:
        """Retry a message.

        Args:
            message (Message): an Alsek message

        Returns:
            None

        Warning:
            * This method will mutate ``message`` by incrementing it.

        """
        if not self.exists(message):
            raise MessageDoesNotExistsError(
                f"Message '{message.uuid}' not found in backend"
            )

        message.increment()
        self.backend.set(self.get_message_name(message), value=message.data)
        self.nack(message)
        log.info(
            "Retrying %s in %s ms...",
            message.summary,
            format(message.get_backoff_duration(), ","),
        )

    def _clear_lock(self, message: Message) -> None:
        lock_name = message._unlink_lock(missing_ok=True)
        if lock_name:
            Lock(lock_name, backend=self.backend).release()

    @magic_logger(
        before=lambda message: log.info("Removing %s...", message.summary),
        after=lambda input_: log.info("Removed %s.", input_["message"].summary),
    )
    def remove(self, message: Message) -> None:
        """Remove a message from the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            None

        """
        self.backend.delete(self.get_message_name(message), missing_ok=True)
        self._clear_lock(message)

    @magic_logger(
        before=lambda message: log.debug("Acking %s...", message.summary),
        after=lambda input_: log.debug("Acked %s.", input_["message"].summary),
    )
    def ack(self, message: Message) -> None:
        """Acknowledge a message by removing it from the data backend.

        Args:
            message (Message): a message to acknowledge

        Returns:
            None

        Warning:
            * Messages will not be redelivered once acked.

        """
        self.remove(message)

    @magic_logger(  # noqa
        before=lambda message: log.debug("Nacking %s...", message.summary),
        after=lambda input_: log.debug("Nacked %s.", input_["message"].summary),
    )
    def nack(self, message: Message) -> None:
        """Do not acknowledge a message and render it eligible
        for redelivery.

        Args:
            message (Message): a message to not acknowledge

        Returns:
            None

        """
        self._clear_lock(message)

    def get_dlq_message_name(self, message: Message) -> str:
        """Get the name for ``message`` in the backend's dead letter queue (DLQ).

        Args:
            message (Message): an Alsek message

        Returns:
            dlq_name (str): message-specific name in the DLQ

        """
        return f"dtq:{self.get_message_name(message)}"

    @magic_logger(
        before=lambda message: log.info("Failing %s...", message.summary),
        after=lambda input_: log.info("Failed %s.", input_["message"].summary),
    )
    def fail(self, message: Message) -> None:
        """Acknowledge and fail a message by removing it from the backend.
        If ``dlq_ttl`` is not null, the messages will be persisted to
        the dead letter queue for the prescribed amount of time.

        Args:
            message (Message): an Alsek message

        Returns:
            None

        """
        self.ack(message)
        if self.dlq_ttl:
            self.backend.set(
                self.get_dlq_message_name(message),
                value=message.data,
                ttl=self.dlq_ttl,
            )
            log.debug("Added %s to DLQ.", message.summary)

    @magic_logger(
        before=lambda message: log.info("Syncing %s...", message.summary),
        after=lambda input_: log.info("Synced %s.", input_["message"].summary),
    )
    def sync(self, message: Message) -> Message:
        """Synchronize a message's internal data with that in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            updated_message (Message): the updated message data

        """
        try:
            data = self.backend.get(self.get_message_name(message), default=Empty)
        except KeyError:
            data = self.backend.get(self.get_dlq_message_name(message), default=Empty)
        return Message(**data)
