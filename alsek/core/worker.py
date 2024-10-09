"""

    Worker Pool

"""

import logging
from collections import defaultdict
from typing import Any, Collection, DefaultDict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from alsek import __version__
from alsek.core.broker import Broker
from alsek.core.consumer import Consumer
from alsek.core.futures import (
    MULTIPROCESSING_BACKEND,
    ProcessTaskFuture,
    TaskFuture,
    ThreadTaskFuture,
)
from alsek.core.message import Message
from alsek.core.task import Task
from alsek.exceptions import MultipleBrokersError, NoTasksFoundError, TerminationError
from alsek.types import SupportedMechanismType
from alsek.utils.checks import has_duplicates
from alsek.utils.logging import magic_logger
from alsek.utils.sorting import dict_sort
from alsek.utils.system import smart_cpu_count

log = logging.getLogger(__name__)


def _extract_broker(tasks: Collection[Task]) -> Broker:
    if not tasks:
        raise NoTasksFoundError("No tasks found")

    brokers = {t.broker for t in tasks}
    if len(brokers) > 1:
        raise MultipleBrokersError("Multiple brokers used")
    else:
        (broker,) = brokers
        return broker


def _derive_consumer_subset(
    tasks: Collection[Task],
    queues: Optional[list[str]],
    task_specific_mode: bool,
) -> dict[str, list[str]]:
    if queues and has_duplicates(queues):
        raise ValueError(f"Duplicates in provided queues: {queues}")
    elif queues and not task_specific_mode:
        return queues

    subset: DefaultDict[str, list[str]] = defaultdict(list)
    for t in sorted(tasks, key=lambda i: i.priority):
        if queues is None or t.queue in queues:
            subset[t.queue].append(t.name)

    if task_specific_mode:
        return dict_sort(subset, key=queues.index if queues else None)
    else:
        return sorted(subset.keys())


class WorkerPool(Consumer):
    """Pool of Alsek workers.

    Generate a pool of workers to service ``tasks``.
    The reference broker is extracted from ``tasks`` and
    therefore must be common among all tasks.

    Args:
        tasks (Collection[Task]): one or more tasks to handle. This
            must include all tasks the worker may encounter by listening
            to ``queues``.
        queues (list[str], optional): the names of one or more queues
            consume messages from. If ``None``, all queues will be consumed.
        task_specific_mode (bool, optional): when defining queues to monitor, include
            tasks names. Otherwise, consider queues broadly.
        max_threads (int): the maximum of tasks with ``mechanism="thread"``
            supported at any 'one' time.
        max_processes (int, optional): the maximum of tasks with ``mechanism="process"``
            supported at any one time. If ``None``, ``max(1, cpu_count() - 1)`` will
            be used.
        management_interval (int): amount of time (in milliseconds) between
            maintenance scans of background task execution.
        slot_wait_interval (int): amount of time (in milliseconds) to wait
            between checks to determine worker availability for pending tasks.
        **kwargs (Keyword Args): Keyword arguments to pass to ``Consumer()``.

    Raises:
        NoTasksFoundError: if no tasks are provided

        MultipleBrokersError: if multiple brokers are
           used by the collected tasks.

    """

    def __init__(
        self,
        tasks: Collection[Task],
        queues: Optional[list[str]] = None,
        task_specific_mode: bool = False,
        max_threads: int = 8,
        max_processes: Optional[int] = None,
        management_interval: int = 100,
        slot_wait_interval: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            broker=_extract_broker(tasks),
            subset=_derive_consumer_subset(
                tasks=tasks,
                queues=queues,
                task_specific_mode=task_specific_mode,
            ),
            **kwargs,
        )
        self.tasks = tasks
        self.queues = queues or sorted(self.subset)
        self.task_specific_mode = task_specific_mode
        self.max_threads = max_threads
        self.max_processes = max_processes or smart_cpu_count()
        self.management_interval = management_interval
        self.slot_wait_interval = slot_wait_interval

        self._task_map = {t.name: t for t in tasks}
        self._futures: dict[str, list[TaskFuture]] = dict(thread=list(), process=list())

        self._pool_manager = BackgroundScheduler()
        self._pool_manager.add_job(
            self._manage_futures,
            trigger=IntervalTrigger(seconds=self.management_interval / 1000),
        )

        if self.max_processes:
            log.debug("Using %s multiprocessing backend.", MULTIPROCESSING_BACKEND)

    def _stop_all_futures(self) -> None:
        for futures in self._futures.values():
            for future in futures:
                future.stop(TerminationError)
                future.clean_up(ignore_errors=True)

    def _manage_futures(self) -> None:
        for mechanism, futures in self._futures.items():
            to_remove = list()
            for future in futures:
                if future.complete:
                    to_remove.append(future)
                elif future.time_limit_exceeded:
                    future.stop(TimeoutError)
                    future.clean_up(ignore_errors=True)
                    to_remove.append(future)

            for f in to_remove:
                self._futures[mechanism].remove(f)

    def _slot_available(self, mechanism: SupportedMechanismType) -> int:
        current_count = len(self._futures[mechanism])
        if mechanism == "thread":
            return current_count < self.max_threads
        else:
            return current_count < self.max_processes

    def _ready(self, message: Message, wait: bool) -> bool:
        while True:
            if self.stop_signal.received:
                return False
            elif self._slot_available(message.mechanism):
                return True
            elif not wait:
                return False
            else:
                self.stop_signal.wait(self.slot_wait_interval)

    def _make_future(self, message: Message) -> TaskFuture:
        task = self._task_map[message.task_name]
        if message.mechanism == "thread":
            return ThreadTaskFuture(task, message=message)
        elif message.mechanism == "process":
            return ProcessTaskFuture(task, message=message)
        else:
            raise ValueError(f"Unsupported mechanism '{message.mechanism}'")

    def _add_future(self, message: Message) -> None:
        if message.task_name in self._task_map:
            self._futures[message.mechanism].append(self._make_future(message))
        else:
            log.error("Unknown task %r in %r.", message.task_name, message.summary)
            self.broker.fail(message)

    @magic_logger(
        before=lambda: log.info("Alsek v%s worker pool booting up...", __version__),
        after=lambda: log.info("Graceful shutdown complete."),
    )
    def run(self) -> None:
        """Run the worker pool.

        This method coordinates the following:

            1.  Starting a background thread to monitor background tasks
            2a. Recovering messages fom the data backend
            2b. Processing recovered messages as places in the pool become available.

        Returns:
            None

        Warning:
            * This method is blocking.

        """
        log.info(
            "Starting worker pool with %s max thread%s and %s max process%s...",
            self.max_threads,
            "(s)" if self.max_threads > 1 else "",
            self.max_processes,
            "(es)" if self.max_processes > 1 else "",
        )
        self._pool_manager.start()
        log.info(
            "Monitoring %s %s.",
            len(self.tasks) if self.task_specific_mode else len(self.queues),
            "tasks" if self.task_specific_mode else "queues"
        )
        log.info("Worker pool online.")

        try:
            for message in self.stream():
                if self._ready(message, wait=True) and self.broker.exists(message):
                    self._add_future(message)
        finally:
            log.info("Worker pool shutting down...")
            self._pool_manager.shutdown()
            self._stop_all_futures()
