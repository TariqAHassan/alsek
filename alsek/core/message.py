"""

    Message

"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable, Optional, Union
from uuid import uuid1

from alsek._defaults import DEFAULT_MECHANISM, DEFAULT_QUEUE, DEFAULT_TASK_TIMEOUT
from alsek.core.backoff import ExponentialBackoff, settings2backoff
from alsek.core.concurrency import Lock
from alsek.types import SupportedMechanismType
from alsek.utils.printing import auto_repr
from alsek.utils.temporal import fromtimestamp_ms, utcnow_timestamp_ms


def _make_uuid() -> str:
    return str(uuid1())


def _collect_callback_uuids(callback_message_data: dict[str, Any]) -> Iterable[str]:
    yield callback_message_data["uuid"]
    if callback_message_data["callback_message_data"]:
        yield from _collect_callback_uuids(
            callback_message_data["callback_message_data"]
        )


class Message:
    """Alsek Message.

    Args:
        task_name (str): the name of the task for which
            the message is intended
        queue (str, optional): the queue for which the message was intended.
            If ``None`` the default queue will be set.
        args (list, tuple, optional): positional arguments to pass to
            the task's function during the execution of ``op()``
        kwargs (dict, optional): keyword arguments to pass to
            the task's function during the execution of ``op()``
        metadata (dict, optional): a dictionary of user-defined message metadata.
            This can store any data types supported by the backend's serializer.
        exception_details (dict, optional): a dictionary of information about exceptions
            with ``name``, ``text`` and ``traceback`` information. See ``ExceptionDetails()``.
        result_ttl (int, optional): time to live (in milliseconds) for the
            result in the result store. If a result store is provided and
            this parameter is ``None``, the result will be persisted indefinitely.
        uuid (str, optional): universal unique identifier for the message.
            If ``None``, one will be generated automatically.
        progenitor_uuid (str, optional): universal unique identifier for the message
            from which this message descended. (This field is only set in for tasks
            with triggers and/or callbacks.)
        retries (int): number of retries
        timeout (int): the maximum amount of time (in milliseconds)
            a task is permitted to run against this message.
        created_at (int): UTC timestamp (in milliseconds) for
            when the message was created
        updated_at (int): UTC timestamp (in milliseconds) for
            when the message was last updated
        delay (int): delay before the message becomes ready
        previous_result (any, optional): the output of any
            previously executed task. (This will only be non-null
            in cases where callbacks are used.)
        previous_message_uuid (str, optional): universal unique identifier
            for the message for the preceding message (This will only be
            non-null in cases where callbacks are used.)
        callback_message_data (dict, optional): data to construct
            a new message as part of a callback operation
        backoff_settings (dict, optional): parameters to control
            backoff. Expected to be of the form
            ``{"algorithm": str, "parameters": dict}``.
        mechanism (SupportedMechanismType): mechanism for executing the task. Must
            be either "process" or "thread".

    Notes:
        * While *not* recommended, ``timeout`` can be disabled,
          in effect, by setting it to a very large integer.

    """

    def __init__(
        self,
        task_name: str,
        queue: Optional[str] = None,
        args: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        kwargs: Optional[dict[Any, Any]] = None,
        metadata: Optional[dict[Any, Any]] = None,
        exception_details: Optional[dict[str, Any]] = None,
        result_ttl: Optional[int] = None,
        uuid: Optional[str] = None,
        progenitor_uuid: Optional[str] = None,
        retries: int = 0,
        timeout: int = DEFAULT_TASK_TIMEOUT,
        created_at: Optional[int] = None,
        updated_at: Optional[int] = None,
        delay: Optional[int] = None,
        previous_result: Optional[Any] = None,
        previous_message_uuid: Optional[str] = None,
        callback_message_data: Optional[dict[str, Any]] = None,
        backoff_settings: Optional[dict[str, int]] = None,
        mechanism: SupportedMechanismType = DEFAULT_MECHANISM,
    ) -> None:
        self.task_name = task_name
        self.queue = queue or DEFAULT_QUEUE
        self.args = tuple(args) if args else tuple()
        self.kwargs = kwargs or dict()
        self.metadata = metadata
        self.exception_details = exception_details
        self.result_ttl = result_ttl
        self.retries = retries
        self.timeout = timeout
        self.uuid = uuid or _make_uuid()
        self.progenitor_uuid = progenitor_uuid
        self.delay = delay or 0
        self.previous_result = previous_result
        self.previous_message_uuid = previous_message_uuid
        self.callback_message_data = callback_message_data
        self.backoff_settings = backoff_settings or ExponentialBackoff().settings
        self.mechanism = mechanism

        if created_at is None and updated_at is None:
            self.created_at = self.updated_at = utcnow_timestamp_ms()
        elif created_at is None or updated_at is None:
            raise ValueError("Time data is corrupt")
        else:
            self.created_at, self.updated_at = created_at, updated_at

        self._lock: Optional[str] = None

    @property
    def data(self) -> dict[str, Any]:
        """Underlying message data."""
        return dict(
            task_name=self.task_name,
            queue=self.queue,
            args=self.args,
            kwargs=self.kwargs,
            metadata=self.metadata,
            exception_details=self.exception_details,
            result_ttl=self.result_ttl,
            uuid=self.uuid,
            progenitor_uuid=self.progenitor_uuid,
            retries=self.retries,
            timeout=self.timeout,
            created_at=self.created_at,
            updated_at=self.updated_at,
            delay=self.delay,
            previous_result=self.previous_result,
            previous_message_uuid=self.previous_message_uuid,
            callback_message_data=self.callback_message_data,
            backoff_settings=self.backoff_settings,
            mechanism=self.mechanism,
        )

    def __repr__(self) -> str:
        params = self.data
        for k in ("created_at", "updated_at"):
            params[k] = fromtimestamp_ms(params[k])
        return auto_repr(self, **params)

    @property
    def summary(self) -> str:
        """High-level summary of the message object."""
        return auto_repr(
            self,
            new_line_threshold=None,
            uuid=self.uuid,
            queue=self.queue,
            task=self.task_name,
        )

    def get_backoff_duration(self) -> int:
        """Get the amount of time to backoff (wait)
        before the message is eligible for processing again,
        should it fail.

        Returns:
            duration (int): duration of the backoff in milliseconds

        """
        return settings2backoff(self.backoff_settings).get(self.retries)

    @property
    def ready_at(self) -> int:
        """Timestamp denoting when the message will be ready for processing."""
        return self.created_at + self.delay + self.get_backoff_duration()

    @property
    def ready(self) -> bool:
        """If the messages is currently ready for processing."""
        return self.ready_at <= utcnow_timestamp_ms()

    @property
    def ttr(self) -> int:
        """Time to ready in milliseconds."""
        if self.ready:
            return 0
        return max(self.ready_at - utcnow_timestamp_ms(), 0)

    @property
    def descendant_uuids(self) -> Optional[list[str]]:
        """A list of uuids which have or will decent from this message."""
        if self.callback_message_data:
            return list(_collect_callback_uuids(self.callback_message_data))
        else:
            return None

    def _link_lock(self, lock: Lock, override: bool = False) -> Message:
        """Link a lock to the current message.

        Links are formed against the ``long_name`` of ``lock``.

        Args:
            lock (Lock): a concurrency lock
            override (bool): if ``True`` replace any existing lock

        Returns:
            message (Message): the updated message

        Warning:
            * Locks links are formed in memory and are
              never persisted to the data backend.

        """
        if self._lock and not override:
            raise AttributeError(f"Already linked to '{self._lock}'")
        else:
            self._lock = lock.long_name
        return self

    def _unlink_lock(self, missing_ok: bool = False) -> Optional[str]:
        """Clear the lock linked to the message.

        Args:
            missing_ok (bool): if ``True`` do not raise
                if no lock is found

        Returns:
            lock (str, optional): the name of the lock which was cleared

        Raises:
            AttributeError: if no lock is associated with the message
                and ``missing_ok`` is not ``True``.

        """
        if self._lock:
            lock = self._lock
            self._lock = None
            return lock
        elif missing_ok:
            return None
        else:
            raise AttributeError("No lock linked to message")

    def clone(self) -> Message:
        """Create an exact copy of the current message.

        Returns:
            clone (Message): the cloned message

        """
        return Message(**deepcopy(self.data))

    def update(self, **data: Any) -> Message:
        """Update the ``data`` in the current message.

        Args:
            **data (Keyword Args): key value pairs of
                data to update

        Returns:
            updated_message (Message): the updated message

        Warning:
            * This method operates 'in place'. To avoid changing the current
              message, first call ``.clone()``, e.g., ``message.clone().update(...)``.
            * Changes are *not* automatically persisted to the backend.

        """
        for k, v in data.items():
            if k in self.data:
                setattr(self, k, v)
            else:
                raise KeyError(f"Unsupported key '{k}'")
        return self

    def duplicate(self, uuid: Optional[str] = None) -> Message:
        """Create a duplicate of the current message, changing only ``uuid``.

        Args:
            uuid (str, optional): universal unique identifier for the new message.
                If ``None``, one will be generated automatically.

        Returns:
            duplicate_message (Message): the duplicate message

        Warning:
            * Linked locks are not conserved

        """
        return self.clone().update(uuid=uuid or _make_uuid())

    def increment(self) -> Message:
        """Update a message by increasing the number
        of retries.

        Returns:
            message (Message): the updated message

        Notes:
            * ``updated_at`` will be updated to the
               current time.

        Warning:
            * Changes are *not* automatically persisted to the backend.

        """
        return self.update(retries=self.retries + 1, updated_at=utcnow_timestamp_ms())
