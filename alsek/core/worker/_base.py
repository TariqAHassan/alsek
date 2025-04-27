"""

    Base Worker Pool

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from alsek import Message
from alsek.core.consumer import Consumer
from alsek.core.task import Task
from alsek.core.worker._helpers import (
    derive_consumer_subset,
    extract_broker,
    filter_tasks,
)
from alsek.types import SupportedMechanismType

log = logging.getLogger(__name__)


class BaseWorkerPool(Consumer, ABC):
    """Pool of Alsek workers.

    Generate a pool of workers to service ``tasks``.
    The reference broker is extracted from ``tasks`` and
    therefore must be common among all tasks.

    Args:
        mechanism (SupportedMechanismType): the mechanism to use (thread or process).
        tasks (list[Task], tuple[Task, ...]): one or more tasks to handle. This
            must include all tasks the worker may encounter by listening
            to ``queues``.
        queues (list[str], optional): the names of one or more queues
            consume messages from. If ``None``, all queues will be consumed.
        task_specific_mode (bool, optional): when defining queues to monitor, include
            tasks names. Otherwise, consider queues broadly.
        slot_wait_interval_seconds (int): Number of milliseconds to wait when the
            pool is saturated before giving other workers a chance and re-scanning
            the queues.
        **kwargs (Keyword Args): Keyword arguments to pass to ``Consumer()``.

    Raises:
        NoTasksFoundError: if no tasks are provided

        MultipleBrokersError: if multiple brokers are
           used by the collected tasks.

    """

    def __init__(
        self,
        *,
        mechanism: SupportedMechanismType,
        tasks: list[Task] | tuple[Task, ...],
        queues: Optional[list[str]] = None,
        task_specific_mode: bool = False,
        slot_wait_interval: int = 50,
        **kwargs: Any,
    ) -> None:
        tasks = filter_tasks(tasks=tasks, mechanism=mechanism)
        super().__init__(
            broker=extract_broker(tasks),
            subset=derive_consumer_subset(
                tasks=tasks,
                queues=queues,
                task_specific_mode=task_specific_mode,
            ),
            **kwargs,
        )
        self.tasks = tasks
        self.queues = queues or sorted(self.subset)
        self.task_specific_mode = task_specific_mode
        self.slot_wait_interval = slot_wait_interval

        self._task_map = {t.name: t for t in tasks}
        self._can_run: bool = True

    @property
    def _slot_wait_interval_seconds(self) -> float:
        return self.slot_wait_interval / 1000

    def on_boot(self) -> None:
        log.info(
            "Monitoring %s %s.",
            len(self.tasks) if self.task_specific_mode else len(self.queues),
            "task(s)" if self.task_specific_mode else "queue(s)",
        )
        log.info("Worker pool online.")

    @abstractmethod
    def prune(self) -> None:
        """Prune spent futures."""
        raise NotImplementedError()

    @abstractmethod
    def submit_message(self, message: Message) -> bool:
        """Handle a single message.

        Args:
            message (Message): an Alsek message.

        Returns:
            submitted (bool): ``True`` if the message was successfully submitted

        """
        raise NotImplementedError()

    def engine(self) -> None:
        """Run the worker pool."""
        while self._can_run and not self.stop_signal.received:
            for message in self.stream():
                self.prune()

                if self.submit_message(message):
                    continue

                # Saturated: free message & retry later
                message.unlink_lock(
                    missing_ok=True,
                    target_backend=self.broker.backend,
                )
                # Brief back-off, then restart the stream (priority reset)
                self.stop_signal.wait(self._slot_wait_interval_seconds)
                # Break so we start the stream again from the beginning.
                # This is important because the stream is ordered by priority.
                # That is, when we finally can acquire a process group again, we
                # want to saturate our capacity by message priority (high -> low).
                break

    @abstractmethod
    def on_shutdown(self) -> None:
        """Stop all active futures."""
        raise NotImplementedError()

    def run(self) -> None:
        """Run the worker pool."""
        self.on_boot()

        try:
            self.engine()
        except KeyboardInterrupt:
            log.info("Keyboard interrupt received. Initiating shutdown...")
        finally:
            log.info("Worker pool shutting down...")
            self.on_shutdown()
