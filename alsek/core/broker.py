"""

    Broker

"""

from __future__ import annotations

import logging
from typing import Any, Optional

import dill

from alsek.core.message import Message
from alsek.defaults import DEFAULT_TTL
from alsek.exceptions import MessageAlreadyExistsError, MessageDoesNotExistsError
from alsek.storage.backends.abstract import Backend
from alsek.types import Empty
from alsek.utils.aggregation import gather_init_params
from alsek.utils.logging import magic_logger
from alsek.utils.namespacing import (
    get_dlq_message_name,
    get_message_name,
    get_priority_namespace_from_message,
)
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

    def serialize(self) -> dict[str, Any]:
        settings = gather_init_params(self, ignore=("backend",))
        settings["backend"] = dict(
            cls=self.backend.__class__,
            encoding=self.backend.encode(),
        )
        return settings

    @classmethod
    def deserialize(cls, settings: dict[str, Any]) -> Broker:
        settings = settings.copy()
        backend_data = dill.loads(settings["backend"]["encoding"])
        settings["backend"] = settings["backend"]["cls"].from_settings(
            backend_data["settings"]
        )
        return cls(**settings)

    def exists(self, message: Message) -> bool:
        """Determine if the message exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            exists (bool): whether the message exists.

        """
        name = get_message_name(message)
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
        name = get_message_name(message)
        try:
            self.backend.set(name, value=message.data, nx=True, ttl=ttl)
        except KeyError:
            raise MessageAlreadyExistsError(f"'{name}' found in backend")

        self.backend.priority_add(
            get_priority_namespace_from_message(message),
            unique_id=name,
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

        # We release the lock before setting the messate data
        # so that the `linked_lock` field on the message ie None.
        message.release_lock(
            not_linked_ok=True,
            target_backend=self.backend,
        )
        message.increment_retries()
        self.backend.set(get_message_name(message), value=message.data)
        log.info(
            "Retrying %s in %s ms...",
            message.summary,
            format(message.get_backoff_duration(), ","),
        )

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
        self.backend.priority_remove(
            key=get_priority_namespace_from_message(message),
            unique_id=get_message_name(message),
        )
        self.backend.delete(get_message_name(message), missing_ok=True)
        message.release_lock(
            not_linked_ok=True,
            target_backend=self.backend,
        )

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
                get_dlq_message_name(message),
                value=message.data,
                ttl=self.dlq_ttl,
            )
            log.debug("Added %s to DLQ.", message.summary)

    @magic_logger(
        before=lambda message: log.info("Failing %s...", message.summary),
        after=lambda input_: log.info("Failed %s.", input_["message"].summary),
    )
    def in_dlq(self, message: Message) -> bool:
        """Determine if a message is in the dead letter queue.

        Args:
            message (Message): an Alsek message

        Returns:
            bool: whether the message is in the DLQ.

        """
        return self.backend.exists(get_dlq_message_name(message))

    @magic_logger(
        before=lambda message: log.info("Syncing from backend %s...", message.summary),
        after=lambda input_: log.info(
            "Synced from backend %s.",
            input_["message"].summary,
        ),
    )
    def sync_from_backend(self, message: Message) -> Message:
        """Synchronize a message's internal data with that in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            updated_message (Message): the updated message data

        """
        try:
            data = self.backend.get(get_message_name(message), default=Empty)
        except KeyError:
            data = self.backend.get(get_dlq_message_name(message), default=Empty)
        return Message(**data)

    @magic_logger(
        before=lambda message: log.info("Syncing %s to backend...", message.summary),
        after=lambda input_: log.info(
            "Synced %s to backend.", input_["message"].summary
        ),
    )
    def sync_to_backend(self, message: Message) -> None:
        """Synchronize the data persisted in the backend with the current state of
        ``message`` held in memory.

        This method is the logical inverse of ``sync_from_backend``; any changes
        made to the ``message`` instance are written back to the backend so that
        future reads reflect the most up-to-date information.

        Args:
            message (Message): an Alsek message whose current state should be
                persisted.

        Returns:
            None

        Warning:
            * This method will mutate ``message`` by updating it
              regardless of whether a lock is linked to it.
              You are responsible for ensuring that any mutation
              of the message's underlying data is only performed
              by the lock owner.

        """
        # Determine which key (regular queue or DLQ) should be updated
        if self.exists(message):
            key = get_message_name(message)
        elif self.in_dlq(message):
            key = get_dlq_message_name(message)
        else:
            raise MessageDoesNotExistsError(
                f"Message '{message.uuid}' not found in backend"
            )

        # Persist the updated message data. We intentionally omit a TTL value
        # to preserve the existing expiry associated with ``key`` (if any).
        self.backend.set(key, value=message.data)
