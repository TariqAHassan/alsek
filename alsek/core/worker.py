"""

    Worker Pool

"""
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from platform import python_implementation
from threading import Thread
from typing import Any, Collection, DefaultDict, Dict, List, Optional, Tuple, Type, cast

import dill
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from alsek import __version__
from alsek._utils.logging import get_logger, magic_logger, setup_logging
from alsek._utils.sorting import dict_sort
from alsek._utils.system import smart_cpu_count, thread_raise
from alsek._utils.temporal import utcnow_timestamp_ms
from alsek.core.broker import Broker
from alsek.core.consumer import Consumer
from alsek.core.message import Message
from alsek.core.task import Task
from alsek.exceptions import MultipleBrokersError, NoTasksFoundError, TerminationError

log = logging.getLogger(__name__)

MULTIPROCESS_BACKEND = (
    os.getenv("ALSEK_MULTIPROCESS_BACKEND", "standard").strip().lower()
)

if MULTIPROCESS_BACKEND == "standard":
    from multiprocessing import Process, Queue
elif MULTIPROCESS_BACKEND == "pytorch":
    from torch.multiprocessing import Process, Queue
else:
    raise ImportError(f"Unsupported multiprocessing backend '{MULTIPROCESS_BACKEND}'")


def _extract_broker(tasks: Collection[Task]) -> Broker:
    if not tasks:
        raise NoTasksFoundError("No tasks found")

    brokers = {t.broker for t in tasks}
    if len(brokers) > 1:
        raise MultipleBrokersError("Multiple brokers used")
    else:
        (broker,) = brokers
        return broker


def _has_duplicates(itera: Collection[Any]) -> bool:
    return len(itera) != len(set(itera))


def _derive_consumer_subset(
    tasks: Collection[Task], queues: Optional[List[str]]
) -> Dict[str, List[str]]:
    if queues and _has_duplicates(queues):
        raise ValueError(f"Duplicates in provided queues: {queues}")

    subset: DefaultDict[str, List[str]] = defaultdict(list)
    for t in sorted(tasks, key=lambda i: i.priority):
        if queues is None or t.queue in queues:
            subset[t.queue].append(t.name)
    return dict_sort(subset, key=queues.index) if queues else dict_sort(subset)


def _generate_callback_message(
    callback_message_data: Dict[str, Any],
    previous_result: Any,
    progenitor: str,
) -> Message:
    data = deepcopy(callback_message_data)
    data["previous_result"] = previous_result
    data["progenitor"] = progenitor
    return Message(**data)


def _future_encoder(task: Task, message: Message) -> bytes:
    return cast(bytes, dill.dumps((task._get_serializable_task(), message)))


def _future_decoder(encoded_data: bytes) -> Tuple[Task, Message]:
    task, message = dill.loads(encoded_data)
    return cast(Task, task), cast(Message, message)


def _retry_future_handler(
    task: Task,
    message: Message,
    exception: BaseException,
) -> None:
    if task.do_retry(message, exception=exception):
        task.broker.retry(message)
    else:
        log.error("Retries exhausted for %s.", message.summary)
        task.broker.fail(message)


def _complete_future_handler(task: Task, message: Message, result: Any) -> None:
    if message.store_result:
        task.result_store.set(message, result=result)  # type: ignore
    if message.callback_message_data and task.do_callback(message, result=result):
        task.broker.submit(
            _generate_callback_message(
                message.callback_message_data,
                previous_result=result,
                progenitor=message.uuid,
            )
        )
    task.broker.ack(message)


class TaskFuture(ABC):
    """Future for background task execution.

    Args:
        task (Task): a task to perform
        message (Message): a message to run ``task`` against

    """

    def __init__(self, task: Task, message: Message) -> None:
        self.task = task
        self.message = message

        self.created_at = utcnow_timestamp_ms()

    @property
    @abstractmethod
    def complete(self) -> bool:
        """Whether or not the task has finished."""
        raise NotImplementedError()

    @property
    def time_limit_exceeded(self) -> bool:
        """Whether or not task has been running longer
        than the allowed time window."""
        if self.complete:
            return False
        return (utcnow_timestamp_ms() - self.created_at) > self.message.timeout

    @abstractmethod
    def stop(self, exception: Type[BaseException]) -> None:
        """Stop the future.

        Args:
            exception (Type[BaseException]): exception type to raise.

        Returns:
            None

        """
        raise NotImplementedError()


class ThreadTaskFuture(TaskFuture):
    """Future for task execution in a separate thread.

    Args:
        task (Task): a task to perform
        message (Message): a message to run ``task`` against

    """

    def __init__(self, task: Task, message: Message) -> None:
        super().__init__(task, message=message)

        self._wrapper_exit: bool = False
        self._thread = Thread(target=self._wrapper, daemon=True)
        self._thread.start()

    @property
    def complete(self) -> bool:
        """Whether or not the task has finished."""
        return not self._thread.is_alive()

    def _wrapper(self) -> None:
        log.info("Received %s...", self.message.summary)

        result, exception = None, None
        try:
            result = self.task.execute(self.message)
            log.info("Successfully processed %s.", self.message.summary)
        except BaseException as error:
            log.error("Error processing %s.", self.message.summary, exc_info=True)
            exception = error

        if self._wrapper_exit:
            log.debug("Thread task future finished after termination.")
        elif exception is not None:
            _retry_future_handler(self.task, self.message, exception=exception)
        else:
            _complete_future_handler(self.task, self.message, result=result)

        self._wrapper_exit = True

    def stop(self, exception: Type[BaseException]) -> None:
        """Stop the future.

        Args:
            exception (Type[BaseException]): exception type to raise.

        Returns:
            None

        """
        if self._thread.ident is None:
            log.error(
                "Thread task future for %s did not start.",
                self.message.summary,
            )
            return None
        elif python_implementation() != "CPython":
            log.error(
                f"Unable to raise exception {exception} in thread {self._thread.ident}. "
                f"Unsupported platform '{python_implementation()}'."
            )
            return None

        thread_raise(self._thread.ident, exception=exception)
        if not self._wrapper_exit:
            self._wrapper_exit = True
            _retry_future_handler(self.task, self.message, exception=exception())


class ProcessTaskFuture(TaskFuture):
    """Future for task execution in a separate process.

    Args:
        task (Task): a task to perform
        message (Message): a message to run ``task`` against
        patience (int): time to wait (in milliseconds) after issuing
            a SIGTERM signal to the process at shutdown. If the process
            is still active after this time, a SIGKILL will be issued.

    """

    def __init__(self, task: Task, message: Message, patience: int = 1 * 1000) -> None:
        super().__init__(task, message=message)
        self.patience = patience

        self._wrapper_exit_queue: Queue = Queue()
        self._process = Process(
            target=self._wrapper,
            args=(
                _future_encoder(task, message=message),
                get_logger().level,
                self._wrapper_exit_queue,
            ),
            daemon=True,
        )
        self._process.start()

    @property
    def complete(self) -> bool:
        """Whether or not the task has finished."""
        return not self._process.is_alive()

    @staticmethod
    def _wrapper(
        encoded_data: bytes,
        log_level: int,
        wrapper_exit_queue: Queue,
    ) -> None:
        setup_logging(log_level)
        task, message = _future_decoder(encoded_data)
        log.info("Received %s...", message.summary)

        result, exception = None, None
        try:
            result = task.execute(message)
            log.info("Successfully processed %s.", message.summary)
        except BaseException as error:
            log.error("Error processing %s.", message.summary, exc_info=True)
            exception = error

        if not wrapper_exit_queue.empty():
            log.debug("Process task future finished after termination.")
        elif exception is not None:
            _retry_future_handler(task, message=message, exception=exception)
        else:
            _complete_future_handler(task, message=message, result=result)

        wrapper_exit_queue.put(1)

    def _shutdown(self) -> None:
        self._process.terminate()
        self._process.join(self.patience / 1000)
        if self._process.is_alive():
            self._process.kill()

    def stop(self, exception: Type[BaseException]) -> None:
        """Stop the future.

        Returns:
            None

        """
        if self._process.ident is None:  # type: ignore
            log.error(
                "Process task future for %s did not start.",
                self.message.summary,
            )
            return None

        self._shutdown()
        if self._wrapper_exit_queue.empty():
            self._wrapper_exit_queue.put(1)
            try:
                raise exception(f"Stopped process {self._process.ident}")  # type: ignore
            except BaseException as error:
                log.error("Error processing %s.", self.message.summary, exc_info=True)
                _retry_future_handler(self.task, self.message, exception=error)


class WorkerPool(Consumer):
    """Pool of Alsek workers.

    Generate a pool of workers to service ``tasks``.
    The reference broker is extracted from ``tasks`` and
    therefore must be common among all tasks.

    Args:
        tasks (Collection[Task]): one or more tasks to handle. This
            must include all tasks the worker may encounter by listening
            to ``queues``.
        queues (List[str], optional): the names of one or more queues
            consume messages from. If ``None``, all queues will be consumed.
        max_threads (int): the maximum of task with ``mechanism="thread"``
            supported at any 'one' time.
        max_processes (int, optional): the maximum of task with ``mechanism="process"``
            supported at any one time. If ``None``, ``max(1, cpu_count() - 1)`` will
            be used.
        management_interval (int): amount of time (in milliseconds) between
            maintenance scans of background task execution.
        slot_wait_interval (int): amount of time (in milliseconds) to wait
            between checks to determine if a process for task execution is available
        **kwargs (Keyword Args): Keyword arguments to pass to ``Consumer()``.

    Raises:
        NoTasksFoundError: if no tasks are provided

        MultipleBrokersError: if multiple brokers are
           used by the collected tasks.

    """

    def __init__(
        self,
        tasks: Collection[Task],
        queues: Optional[List[str]] = None,
        max_threads: int = 8,
        max_processes: Optional[int] = None,
        management_interval: int = 100,
        slot_wait_interval: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            broker=_extract_broker(tasks),
            subset=_derive_consumer_subset(tasks, queues=queues),
            **kwargs,
        )
        self.tasks = tasks
        self.queues = queues
        self.max_threads = max_threads
        self.max_processes = max_processes or smart_cpu_count()
        self.management_interval = management_interval
        self.slot_wait_interval = slot_wait_interval

        self._task_map = {t.name: t for t in tasks}
        self._futures: Dict[str, List[TaskFuture]] = dict(thread=list(), process=list())

        self._pool_manager = BackgroundScheduler()
        self._pool_manager.add_job(
            self._manage_futures,
            trigger=IntervalTrigger(seconds=self.management_interval / 1000),
        )
        self._pool_manager.start()

    def _stop_all_futures(self) -> None:
        for futures in self._futures.values():
            for future in futures:
                future.stop(TerminationError)

    def _manage_futures(self) -> None:
        for mechanism, futures in self._futures.items():
            to_remove = list()
            for future in futures:
                if future.complete:
                    to_remove.append(future)
                elif future.time_limit_exceeded:
                    future.stop(TimeoutError)
                    to_remove.append(future)

            for f in to_remove:
                self._futures[mechanism].remove(f)

    def _slot_available(self, mechanism: str) -> int:
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

    @magic_logger(
        before=lambda: log.info("Alsek v%s worker pool booting up...", __version__),
        after=lambda: log.info("Graceful shutdown complete."),
    )
    def start(self) -> None:
        """Start the worker pool.

        This method coordinates the following:

            1.  Starting a background thread to monitor background tasks
            2a. Recovering messages fom the data backend
            2b. Processing recovered messages as places in the pool become available.

        Returns:
            None

        """
        self._pool_manager.start()
        log.info("Worker pool online.")
        for message in self.stream():
            if self._ready(message, wait=True):
                self._futures[message.mechanism].append(self._make_future(message))

        log.info("Worker pool shutting down...")
        self._pool_manager.shutdown()
        self._stop_all_futures()
