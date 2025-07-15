"""

    Async Status Tracker

"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterable, Optional, cast

import dill

from alsek.core.message import Message
from alsek.core.status.abstract import BaseStatusTracker
from alsek.core.status.types import TERMINAL_TASK_STATUSES, StatusUpdate, TaskStatus
from alsek.exceptions import ValidationError
from alsek.storage.backends.abstract import AsyncBackend


class AsyncStatusTracker(BaseStatusTracker):
    __doc__ = f"Async {BaseStatusTracker.__doc__}"

    def __init__(self, backend: AsyncBackend, **kwargs: Any) -> None:
        super().__init__(backend=backend, **kwargs)
        # Redundant, but needed for the PyCharm's type
        # checker to understand that self.backend is `AsyncBackend`.
        self.backend = cast(AsyncBackend, self.backend)

        if not self.backend.IS_ASYNC:
            raise AssertionError("Backend is not async")

    @staticmethod
    def deserialize(data: dict[str, Any]) -> AsyncStatusTracker:
        backend_data = dill.loads(data["backend"])
        backend = backend_data["backend"].from_settings(backend_data["settings"])
        return AsyncStatusTracker(
            backend=backend,
            ttl=data["ttl"],
            enable_pubsub=data["enable_pubsub"],
        )

    async def exists(self, message: Message) -> bool:
        """Check if a status for ``message`` exists in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            bool

        """
        return await self.backend.exists(self.get_storage_name(message))

    async def publish_update(self, message: Message, update: StatusUpdate) -> None:
        """Publish a PUBSUB update for a message.

        Args:
            message (Message): an Alsek message
            update (StatusUpdate): a status to publish

        Returns:
            None

        """
        await self.backend.pub(
            self.get_pubsub_name(message),
            value=update.as_dict(),  # converting to dict makes this serializer-agnostic
        )

    async def listen_to_updates(
        self,
        message: Message,
        auto_exit: bool = True,
    ) -> AsyncIterable[StatusUpdate]:
        """Listen to PUBSUB updates for ``message``.

        Args:
            message (Message): an Alsek message
            auto_exit (bool): if ``True`` stop listening if a terminal status for the
                task is encountered (succeeded or failed).

        Returns:
            stream (AsyncIterable[StatusUpdate]): A stream of updates from the pubsub channel

        """
        if not self.enable_pubsub:
            raise ValueError("PUBSUB not enabled")

        async for i in self.backend.sub(self.get_pubsub_name(message)):
            if i.get("type", "").lower() == "message":
                update = StatusUpdate(
                    status=TaskStatus[i["data"]["status"]],  # noqa
                    details=i["data"]["details"],  # noqa
                )
                yield update
                if auto_exit and update.status in TERMINAL_TASK_STATUSES:
                    break

    async def set(
        self,
        message: Message,
        status: TaskStatus,
        details: Optional[Any] = None,
    ) -> None:
        """Set a ``status`` for ``message``.

        Args:
            message (Message): an Alsek message
            status (TaskStatus): a status to set
            details (Any, optional): additional information about the status (e.g., progress percentage)

        Returns:
            None

        """
        update = StatusUpdate(status=status, details=details)
        await self.backend.set(
            self.get_storage_name(message),
            value=update.as_dict(),
            ttl=self.ttl if status == TaskStatus.SUBMITTED else None,
        )
        if self.enable_pubsub:
            await self.publish_update(message, update=update)

    async def get(self, message: Message) -> StatusUpdate:
        """Get the status of ``message``.

        Args:
            message (Message): an Alsek message

        Returns:
            status (StatusUpdate): the status of ``message``

        """
        if value := await self.backend.get(self.get_storage_name(message)):
            return StatusUpdate(
                status=TaskStatus[value["status"]],  # noqa
                details=value["details"],
            )
        else:
            raise KeyError(f"No status found for message '{message.summary}'")

    async def wait_for(
        self,
        message: Message,
        status: TaskStatus | tuple[TaskStatus, ...] | list[TaskStatus],
        timeout: Optional[float] = 5.0,
        poll_interval: float = 0.05,
        raise_on_timeout: bool = True,
    ) -> TaskStatus:
        """Wait for a message to reach a desired status.

        Args:
            message (Message): the message to monitor
            status (TaskStatus, tuple[TaskStatus...], list[TaskStatus]): the target status
            timeout (float, optional): max time to wait (in seconds). None means wait forever.
            poll_interval (float): how often to check (in seconds)
            raise_on_timeout (bool): if ``True`` raise a ``TimeoutError`` if waiting times out
                otherwise return the current status

        Returns:
            status (TaskStatus): the status of ``message`` after waiting

        """
        current_status: Optional[TaskStatus] = None
        if not isinstance(status, TaskStatus) and not isinstance(status, (list, tuple)):
            raise ValueError(f"Invalid status type: {type(status)}")

        def is_current_status_match() -> bool:
            if current_status is None:
                return False
            elif isinstance(status, TaskStatus):
                return current_status == status
            elif isinstance(status, (list, tuple)):
                return current_status in status
            else:
                raise ValueError(f"Invalid status type: {type(status)}")

        deadline = None if timeout is None else time.time() + timeout
        while True:
            try:
                current_status = (await self.get(message)).status
                if is_current_status_match():
                    return current_status
            except KeyError:
                pass
            if deadline is not None and time.time() > deadline:
                if raise_on_timeout:
                    raise TimeoutError(f"Timeout waiting for '{message.summary}'")
                else:
                    return TaskStatus.UNKNOWN if current_status is None else current_status  # fmt: skip
            await asyncio.sleep(poll_interval)

    async def delete(self, message: Message, check: bool = True) -> None:
        """Delete the status of ``message``.

        Args:
            message (Message): an Alsek message
            check (bool): check that it is safe to delete the status.
                This is done by ensuring that the current status of ``message``
                is terminal (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``).

        Returns:
            None

        Raises:
            ValidationError: if ``check`` is ``True`` and the status of
                ``message`` is not ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``.

        """
        if check and (await self.get(message)).status not in TERMINAL_TASK_STATUSES:
            raise ValidationError(f"Message '{message.uuid}' in a non-terminal state")
        await self.backend.delete(self.get_storage_name(message), missing_ok=False)
