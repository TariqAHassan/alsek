"""

    Futures

"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from platform import python_implementation
from threading import Thread
from typing import Any, Type, cast

import dill

from alsek import Message
from alsek.core import Event, Process, Queue
from alsek.core.status import TaskStatus
from alsek.core.task import Task
from alsek.exceptions import RevokedError, TerminationError
from alsek.utils.decorators import suppress_exception
from alsek.utils.logging import get_logger, setup_logging
from alsek.utils.parsing import parse_exception
from alsek.utils.system import thread_raise
from alsek.utils.temporal import utcnow_timestamp_ms

log = logging.getLogger(__name__)


def _generate_callback_message(
    callback_message_data: dict[str, Any],
    previous_result: Any,
    progenitor_uuid: str,
    previous_message_uuid: str,
) -> Message:
    data = deepcopy(callback_message_data)
    data["previous_result"] = previous_result
    data["progenitor_uuid"] = progenitor_uuid
    data["previous_message_uuid"] = previous_message_uuid
    return Message(**data)


def _process_future_encoder(task: Task, message: Message) -> bytes:
    return cast(bytes, dill.dumps((task.serialize(), message)))


def _process_future_decoder(encoded_data: bytes) -> tuple[Task, Message]:
    task_data, message = dill.loads(encoded_data)
    return Task.deserialize(task_data), cast(Message, message)


def _handle_retry(
    task: Task,
    message: Message,
    exception: BaseException,
    update_exception_on_message: bool = True,
) -> None:
    if update_exception_on_message:
        message.update(exception_details=parse_exception(exception).as_dict())

    try:
        task.on_retry(message, exception=exception)
    except BaseException:  # noqa
        log.critical("Error encountered with `on_retry()`", exc_info=True)

    task.broker.retry(message)
    task.update_status(message, status=TaskStatus.RETRYING)


def _handle_failure(
    task: Task,
    message: Message,
    exception: BaseException,
    update_exception_on_message: bool = True,
) -> None:
    if update_exception_on_message:
        message.update(exception_details=parse_exception(exception).as_dict())

    try:
        task.on_failure(message, exception=exception)
    except BaseException:  # noqa
        log.critical("Error encountered with `on_failure()`", exc_info=True)

    task.broker.fail(message)
    task.update_status(message, status=TaskStatus.FAILED)


def _error_encountered_future_handler(
    task: Task,
    message: Message,
    exception: BaseException,
    update_exception_on_message: bool = True,
) -> None:
    if task.is_revoked(message):
        try:
            task.on_revocation(message, exception=exception, result=None)
        except BaseException:  # noqa
            log.critical("Error encountered with `on_revocation()`", exc_info=True)
        return None

    if task.do_retry(message, exception=exception):
        _handle_retry(
            task=task,
            message=message,
            exception=exception,
            update_exception_on_message=update_exception_on_message,
        )
    elif not task.broker.in_dlq(message):
        log.error("Retries exhausted for %s.", message.summary)
        _handle_failure(
            task=task,
            message=message,
            exception=exception,
            update_exception_on_message=update_exception_on_message,
        )


def _complete_future_handler(task: Task, message: Message, result: Any) -> None:
    if task.is_revoked(message):
        try:
            task.on_revocation(message, exception=None, result=result)
        except BaseException:  # noqa
            log.critical("Error encountered with `on_revocation()`", exc_info=True)
        return None

    if task.result_store:
        task.result_store.set(message, result=result)
    if message.callback_message_data and task.do_callback(message, result=result):
        task.broker.submit(
            _generate_callback_message(
                message.callback_message_data,
                previous_result=result,
                progenitor_uuid=message.progenitor_uuid or message.uuid,
                previous_message_uuid=message.uuid,
            )
        )
    task.broker.ack(message)
    task.update_status(message, status=TaskStatus.SUCCEEDED)
    try:
        task.on_success(message, result=result)
    except BaseException:  # noqa
        log.critical("Error encountered with `on_success()`", exc_info=True)


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

        self._revocation_stop_event = Event()
        self._revocation_scan_thread = Thread(
            target=self._revocation_scan,
            daemon=True,
        )

    @property
    @abstractmethod
    def complete(self) -> bool:
        """Whether the task has finished."""
        raise NotImplementedError()

    @property
    def time_limit_exceeded(self) -> bool:
        """Whether task has been running longer
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

    @suppress_exception(
        TerminationError,
        on_suppress=lambda error: log.info("Termination Detected"),
    )
    def _revocation_scan(self, check_interval: int | float = 0.5) -> None:
        while not self.complete and not self._revocation_stop_event.is_set():
            if self.task.is_revoked(self.message):
                log.info(
                    "Evicting '%s' due to task revocation...",
                    self.message.summary,
                )
                self.stop(RevokedError)
                _handle_failure(
                    task=self.task,
                    message=self.message,
                    exception=RevokedError(f"Task '{self.task.name}' was revoked."),
                )
                log.info("Evicted '%s'.", self.message.summary)
                break
            self._revocation_stop_event.wait(check_interval)

    def clean_up(self, ignore_errors: bool = False) -> None:
        try:
            self._revocation_stop_event.set()
            self._revocation_scan_thread.join(timeout=0)
        except BaseException as error:  # noqa
            log.error(
                "Clean up error encountered for task %s with message %s.",
                self.task.name,
                self.message.summary,
            )
            if not ignore_errors:
                raise error


class ThreadTaskFuture(TaskFuture):
    """Future for task execution in a separate thread.

    Args:
        task (Task): a task to perform
        message (Message): a message to run ``task`` against
        complete_only_on_thread_exit (bool): if ``True``, only mark the future
            as complete when the thread formally exits (i.e., is not alive).
            Pro: more rigorous — avoids marking the task complete until the thread fully terminates.
            Useful when you need strict control over thread lifecycle (e.g., for resource management).
            Con: may lead to hanging if the thread doesn't terminate quickly (e.g., when using
            `thread_raise()` during revocation). This can also temporarily result in more than the
            allotted number of threads running, because it entails treating a thread as
            expired regardless of its actual status.

    """

    def __init__(
        self,
        task: Task,
        message: Message,
        complete_only_on_thread_exit: bool = False,
    ) -> None:
        super().__init__(task, message=message)
        self.complete_only_on_thread_exit = complete_only_on_thread_exit

        self._wrapper_exit: bool = False
        self._thread = Thread(target=self._wrapper, daemon=True)
        self._thread.start()

        # Note: this must go here b/c the scan depends on
        #   `.complete`, which in turn depends on `_thread`.
        self._revocation_scan_thread.start()

    @property
    def complete(self) -> bool:
        """Whether the task has finished."""
        thread_alive = self._thread.is_alive()
        if self.complete_only_on_thread_exit:
            return not thread_alive
        else:
            # If _wrapper_exit is True, consider the task complete even if the thread is still running
            # This ensures the future gets removed from the worker pool's _futures list
            # and new tasks can be polled even if a revoked task's thread is still running
            return self._wrapper_exit or not thread_alive

    def _wrapper(self) -> None:
        log.info("Received %s...", self.message.summary)
        self.task.update_status(self.message, status=TaskStatus.RUNNING)

        result, exception = None, None
        try:
            self.task.pre_op(self.message)
            result = self.task.execute(self.message)
            if self.task.is_revoked(self.message):
                log.info(
                    "Result for %s recovered after revocation. Discarding.",
                    self.message.summary,
                )
                return None

            self.message.update(exception_details=None)  # clear any existing errors
            log.info("Successfully processed %s.", self.message.summary)
        except BaseException as error:
            log.error("Error processing %s.", self.message.summary, exc_info=True)
            exception = error
            self.message.update(exception_details=parse_exception(exception).as_dict())

        # Post op is called here so that exception_details can be set
        self.task.post_op(self.message, result=result)

        if self._wrapper_exit:
            log.debug("Thread task future finished after termination.")
        elif exception is not None:
            _error_encountered_future_handler(
                task=self.task,
                message=self.message,
                exception=exception,
            )
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
            _error_encountered_future_handler(
                self.task,
                message=self.message,
                exception=exception(f"Stopped thread {self._thread.ident}"),
            )


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
                _process_future_encoder(task, message=message),
                get_logger().level,
                self._wrapper_exit_queue,
            ),
            daemon=True,
        )
        self._process.start()

        # Note: this must go here b/c the scan depends on
        #   `.complete`, which in turn depends on `_process`.
        self._revocation_scan_thread.start()

    @property
    def complete(self) -> bool:
        """Whether the task has finished."""
        return not self._process.is_alive()

    @staticmethod
    def _wrapper(
        encoded_data: bytes,
        log_level: int,
        wrapper_exit_queue: Queue,
    ) -> None:
        setup_logging(log_level)
        task, message = _process_future_decoder(encoded_data)
        log.info("Received %s...", message.summary)
        task.update_status(message, status=TaskStatus.RUNNING)

        result, exception = None, None
        try:
            task.pre_op(message)
            result = task.execute(message)
            if task.is_revoked(message):
                log.info(
                    "Result for %s recovered after revocation. Discarding.",
                    message.summary,
                )
                return None

            message.update(exception_details=None)  # clear any existing errors
            log.info("Successfully processed %s.", message.summary)
        except BaseException as error:
            log.error("Error processing %s.", message.summary, exc_info=True)
            exception = error
            message.update(exception_details=parse_exception(exception).as_dict())

        # Post op is called here so that exception_details can be set
        task.post_op(message, result=result)

        if not wrapper_exit_queue.empty():
            log.debug("Process task future finished after termination.")
        elif exception is not None:
            _error_encountered_future_handler(
                task,
                message=message,
                exception=exception,
                update_exception_on_message=False,
            )
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
                _error_encountered_future_handler(
                    self.task, self.message, exception=error
                )
