"""

    Task

"""

from __future__ import annotations

import inspect
import logging
from functools import partial
from typing import Any, Callable, Optional, Type, Union, cast

import dill
from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek.core.backoff import Backoff, ConstantBackoff, ExponentialBackoff
from alsek.core.broker import Broker
from alsek.core.message import Message
from alsek.core.status import TERMINAL_TASK_STATUSES, StatusTracker, TaskStatus
from alsek.defaults import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_MECHANISM,
    DEFAULT_QUEUE,
    DEFAULT_TASK_TIMEOUT,
    DEFAULT_TTL,
)
from alsek.exceptions import RevokedError, SchedulingError, ValidationError
from alsek.storage.result import ResultStore
from alsek.types import SUPPORTED_MECHANISMS, SupportedMechanismType
from alsek.utils.aggregation import gather_init_params
from alsek.utils.logging import magic_logger
from alsek.utils.namespacing import get_message_name
from alsek.utils.parsing import ExceptionDetails, get_exception_name
from alsek.utils.printing import auto_repr

log = logging.getLogger(__name__)


def _expects_message(function: Callable[..., Any]) -> bool:
    parameters = inspect.signature(function).parameters
    if "message" in parameters:
        if parameters["message"].annotation != inspect._empty:  # type: ignore
            return bool(parameters["message"].annotation == Message)
        else:
            return True
    else:
        return False


def _collapse_callbacks(callbacks: tuple[Message, ...]) -> Message:
    # Note: this function operates 'in place' on the items in
    # `callbacks`. This is done so that callback information
    # will become available on the original messages.
    if any(c.callback_message_data for c in callbacks):
        raise AttributeError(f"Existing callbacks in {callbacks}")

    for i in range(1, len(callbacks))[::-1]:
        callbacks[i - 1].update(callback_message_data=callbacks[i].data)
    return callbacks[0]


def _parse_callback(callback: Union[Message, tuple[Message, ...]]) -> Message:
    if isinstance(callback, Message):
        return callback
    else:
        return _collapse_callbacks(callback)


class _MultiSubmit:
    def __init__(
        self,
        message: Message,
        broker: Broker,
        on_submit: Optional[Callable[[Message], None]] = None,
        callback_op: Optional[Callable[[Message], None]] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> None:
        self.message = message
        self.broker = broker
        self.on_submit = on_submit
        self.callback_op = callback_op
        self.options = options

        self.first: bool = True

    def _get_message(self) -> Message:
        return (
            self.message
            if self.first
            else self.message.duplicate().update(progenitor_uuid=self.message.uuid)
        )

    def __call__(self) -> None:
        message = self._get_message()
        if self.on_submit:
            self.on_submit(message)
        self.broker.submit(message, **(self.options or dict()))
        self.first = False
        if self.callback_op:
            self.callback_op(message)


class Task:
    """Alsek Task.

    Args:
        function (callable): function to use for the main operation
        broker (Broker): an Alsek broker
        name (str, optional): the name of the task. If ``None``,
            the class name will be used.
        queue (str, optional): the name of the queue to generate the task on.
            If ``None``, the default queue will be used.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff, optional): backoff algorithm and parameters to use when computing
            delay between retries
        result_store (ResultStore, optional): store for persisting task results
        status_tracker (StatusTracker, optional): store for persisting task statuses
        mechanism (SupportedMechanismType): mechanism for executing the task. Must
            be either "process" or "thread".
        no_positional_args (bool): if ``True``, the task will not accept positional arguments.

    Notes:
        * ``do_retry()`` can be overridden in cases where ``max_retries``
           is not sufficiently complex to determine if a retry should occur.

    Warning:
        * Timeouts are not supported for ``mechanism='thread'`` on Python
          implementations other than CPython.

    """

    def __init__(
        self,
        function: Callable[..., Any],
        broker: Broker,
        name: Optional[str] = None,
        queue: Optional[str] = None,
        timeout: int = DEFAULT_TASK_TIMEOUT,
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        backoff: Optional[Backoff] = ExponentialBackoff(),
        result_store: Optional[ResultStore] = None,
        status_tracker: Optional[StatusTracker] = None,
        mechanism: SupportedMechanismType = DEFAULT_MECHANISM,
        no_positional_args: bool = False,
    ) -> None:
        self.function = function
        self.broker = broker
        self.queue = queue or DEFAULT_QUEUE
        self.timeout = timeout
        self._name = name
        self.max_retries = max_retries
        self.backoff = backoff or ConstantBackoff(
            constant=0,
            floor=0,
            ceiling=0,
            zero_override=True,
        )
        self.result_store = result_store
        self.status_tracker = status_tracker
        self.mechanism = mechanism
        self.no_positional_args = no_positional_args

        if mechanism not in SUPPORTED_MECHANISMS:
            raise ValueError(f"Unsupported mechanism '{mechanism}'")

        self._deferred: bool = False

    def serialize(self) -> dict[str, Any]:
        settings = gather_init_params(self, ignore=("broker",))

        # Broker
        settings["broker"] = gather_init_params(self.broker, ignore=("backend",))
        settings["broker"]["backend"] = self.broker.backend.encode()

        # Status Tracker
        if self.status_tracker:
            settings["status_tracker"] = self.status_tracker.serialize()

        # Result Store
        if self.result_store:
            settings["result_store"] = self.result_store.serialize()

        return dict(task=self.__class__, settings=settings)

    @staticmethod
    def deserialize(data: dict[str, Any]) -> Task:
        def unwind_settings(settings: dict[str, Any]) -> dict[str, Any]:
            backend_data = dill.loads(settings["broker"]["backend"])

            # Broker
            settings["broker"]["backend"] = backend_data["backend"]._from_settings(
                backend_data["settings"]
            )
            settings["broker"] = Broker(**settings["broker"])

            # Status Tracker
            if status_tracker_settings := settings.get("status_tracker"):
                settings["status_tracker"] = StatusTracker.deserialize(status_tracker_settings)  # fmt: skip

            # Result Store
            if result_store_settings := settings.get("result_store"):
                settings["result_store"] = ResultStore.deserialize(
                    result_store_settings
                )

            return settings

        rebuilt_task = data["task"](**unwind_settings(data["settings"]))
        return cast(Task, rebuilt_task)

    @property
    def name(self) -> str:
        """Name of the task."""
        return self._name if self._name else self.function.__name__

    def __repr__(self) -> str:
        return auto_repr(
            self,
            function=self.function,
            broker=self.broker,
            name=self.name,
            queue=self.queue,
            max_retries=self.max_retries,
            backoff=self.backoff,
            mechanism=self.mechanism,
            deferred_mode="enabled" if self._deferred else "disabled",
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)

    def _update_status(self, message: Message, status: TaskStatus) -> None:
        if self.status_tracker:
            log.debug(f"Setting status of '{message.uuid}' to {status}...")
            self.status_tracker.set(message, status=status)

    @property
    def deferred(self) -> bool:
        """Whether deferred mode is currently enabled."""
        return self._deferred

    def defer(self) -> Task:
        """Enter "deferred" mode.

        Do not submit the next message created by ``generate()``
        to the broker.

        Returns:
            task (Task): the current task

        Warning:
            * Deferred mode is automatically cancelled by ``generate()``
              prior to it returning.

        """
        self._deferred = True
        return self

    def cancel_defer(self) -> Task:
        """Cancel "deferred" mode.

        Returns:
            task (Task): the current task

        """
        self._deferred = False
        return self

    def on_submit(self, message: Message) -> None:
        """Handles the action to be performed when a message is submitted.
        This method processes  the provided message and executes the required
        behavior upon submission.

        Args:
            message (Message): The message object submitted for processing.

        Returns:
            None

        """

    def _submit(self, message: Message, **options: Any) -> None:
        self.broker.submit(message, **options)

    def generate(
        self,
        args: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        kwargs: Optional[dict[Any, Any]] = None,
        priority: int = 0,
        metadata: Optional[dict[Any, Any]] = None,
        result_ttl: Optional[int] = None,
        uuid: Optional[str] = None,
        timeout_override: Optional[int] = None,
        delay: Optional[int] = None,
        previous_result: Any = None,
        callback: Optional[Union[Message, tuple[Message, ...]]] = None,
        queue: Optional[str] = None,
        submit: bool = True,
        **options: Any,
    ) -> Message:
        """Generate an instance of the task for processing.

        This method generates a new message for the task and
        submit it to the broker.

        Args:
            args (list, tuple, optional): positional arguments to pass to ``function``
            kwargs (dict, optional): keyword arguments to pass to ``function``
            priority (int): priority of the message within the task.
                Messages with lower values will be executed before messages with higher values.
            metadata (dict, optional): a dictionary of user-defined message metadata.
                This can store any data types supported by the backend's serializer.
            result_ttl (int, optional): time to live (in milliseconds) for the
                result in the result store. If a result store is provided and
            this parameter is ``None``, the result will be persisted indefinitely.
            uuid (str, optional): universal unique identifier for the message.
                If ``None``, one will be generated automatically.
            timeout_override (int, optional): override the default maximum runtime
                (in milliseconds) for instances of this task.
            set maximum amount of time (in milliseconds)
                this message is permitted to run. This will override the default
                for the task.
            delay (int, optional): delay before message is ready (in milliseconds)
            previous_result (Any): result of a previous task.
            callback (Message, tuple[Message, ...], optional): one or more messages
                to be submitted to the broker after the proceeding message has been
                successfully processed by a worker.
            queue (str, optional): queue to use for the task. If none, the default
                queue for this task will be used.
            submit (bool): if ``True`` submit the task to the broker
            options (Keyword Args): options to use when submitting
                the message via the broker. See ``Broker.submit()``.

        Returns:
            message (Message): message generated for the task

        Warning:
            * ``submit`` is overridden to ``False`` if deferred mode is active
            * ``uuid`` is refreshed after the first event when using a trigger.
            * If manually overriding ``queue`` such that it differs from the default
              for this task, Worker Pools built using ``task_specific_mode=True`` will
              fail acknowledge its existence.

        """
        if result_ttl and not self.result_store:
            raise ValidationError(f"`result_ttl` invalid. No result store set.")
        elif args and self.no_positional_args:
            raise ValidationError(f"Task does not accept positional arguments.")
        elif not isinstance(priority, int) or priority < 0:
            raise ValueError("`priority` must be an int greater than or equal to zero")

        message = Message(
            task_name=self.name,
            queue=queue or self.queue,
            args=args,
            kwargs=kwargs,
            priority=priority,
            metadata=metadata,
            result_ttl=result_ttl,
            uuid=uuid,
            timeout=timeout_override or self.timeout,
            delay=delay,
            previous_result=previous_result,
            callback_message_data=_parse_callback(callback).data if callback else None,
            backoff_settings=self.backoff.settings,
            mechanism=self.mechanism,
        )
        if self._deferred:
            self.cancel_defer()
        elif submit:
            self._submit(message, **options)
            self._update_status(message, status=TaskStatus.SUBMITTED)
            self.on_submit(message)
        return message

    def pre_op(self, message: Message) -> None:
        """Operation to perform before running ``op``.

        Args:
            message (Message): message ``op`` will be run against.

        Returns:
            None

        """

    def op(self, message: Message) -> Any:
        """Pass ``message`` data to ``function`` for processing.

        Args:
            message (Message): message to perform the operation against

        Returns:
            result (Any): output of the function

        Notes:
            * If, and only if, the signature of ``function`` contains
              a ``message`` parameter, ``message`` itself will be passed
              along with any ``args`` and ``kwargs`` contained in the message.

        Warning:
            * ``message`` will not be passed in cases where a "message"
              exists in ``message.kwargs``.

        """
        if "message" not in message.kwargs and _expects_message(self.function):
            return self.function(*message.args, **message.kwargs, message=message)
        else:
            return self.function(*message.args, **message.kwargs)

    def post_op(self, message: Message, result: Any) -> None:
        """Operation to perform after running ``op``.

        Args:
            message (Message): message ``op()`` was run against
            result (Any): output of ``op()``

        Returns:
            None

        """

    def on_revocation(
        self, message: Message, exception: Optional[BaseException], result: Any
    ) -> None:
        """Handles the event when a message is revoked and logs the associated exception.

        Args:
            message: The message instance that was revoked.
            exception: The exception instance that represents the reason for the
                revocation.
            result (Any): The result of the revoked operation. This is only provided
                if the task succeeds

        Returns:
            None

        """

    def on_retry(self, message: Message, exception: BaseException) -> None:
        """Handles the retry logic when a processing failure occurs.

        Args:
            message: The message object that failed during processing and is
                subject to a retry attempt.
            exception: The exception instance that was raised during the
                failure of processing the message.

        Returns:
            None

        """

    def on_failure(self, message: Message, exception: BaseException) -> None:
        """
        Handles the actions to be performed when an operation fails.

        Args:
            message: The message object containing the details of the failed
                operation.
            exception: The exception object associated with the failure, providing
                additional context or details about what caused the failure.

        Returns:
            None

        """

    def on_success(self, message: Message, result: Any) -> None:
        """Handles successful outcomes of an operation by processing the given message
        and corresponding result.

        Args:
            message: An instance of the Message class that contains relevant information
                about the operation.
            result: The result of the completed operation, which can be of any type.

        Returns:
            None

        """

    def execute(self, message: Message) -> Any:
        """Execute the task against a message.

        Args:
            message (Message): message to process

        Returns:
            result (Any): output of ``op()``

        """
        return self.op(message)

    def do_retry(self, message: Message, exception: BaseException) -> bool:  # noqa
        """Whether a failed task should be retried.

        Args:
            message (Message): message which failed
            exception (BaseException): the exception which was raised

        Returns:
            bool

        """
        if self.is_revoked(message):
            return False
        return self.max_retries is None or message.retries < self.max_retries

    def do_callback(self, message: Message, result: Any) -> bool:  # noqa
        """Whether or to submit the callback provided.

        Args:
            message (Message): message with the callback
            result (Any): output of ``op()``

        Returns:
            bool

        Warning:
            * If the task message does not have a callback this
              method will *not* be invoked.

        """
        return True

    @staticmethod
    def _make_revoked_key_name(message: Message) -> str:
        return f"revoked:{get_message_name(message)}"

    @magic_logger(
        before=lambda message: log.info("Revoking %s...", message.summary),
        after=lambda input_: log.debug("Revoked %s.", input_["message"].summary),
    )
    def revoke(self, message: Message, skip_if_running: bool = False) -> None:
        """Revoke the task.

        Args:
            message (Message): message to revoke
            skip_if_running (bool): if ``True``, skip revoking the task if it is currently RUNNING.
                Notes: requires ``status_tracker`` to be set.

        Returns:
            None

        """
        log.info("Revoking %s...", message.summary)
        if (
            self.status_tracker
            and self.status_tracker.get(message).status in TERMINAL_TASK_STATUSES
        ):
            log.info("Message is already terminal: %s", message.summary)
            return
        elif skip_if_running and not self.status_tracker:
            raise AttributeError("`skip_if_running` requires `status_tracker` to be set")  # fmt: skip
        elif skip_if_running and (
            (status_update := self.status_tracker.get(message))
            and status_update.status == TaskStatus.RUNNING
        ):
            log.info("Message is currently running: %s", message.summary)
            return

        self.broker.backend.set(
            self._make_revoked_key_name(message),
            value=True,
            ttl=DEFAULT_TTL,
        )
        message.update(
            exception_details=ExceptionDetails(
                name=get_exception_name(RevokedError),
                text="Task Revoked",
                traceback=None,
            ).as_dict()
        )
        self._update_status(message, status=TaskStatus.FAILED)
        self.broker.fail(message)

    def is_revoked(self, message: Message) -> bool:
        """Check if a message is revoked.

        Args:
            message (Message): an Alsek message

        Returns:
            None

        """
        if self.broker.backend.get(self._make_revoked_key_name(message)):
            return True
        else:
            return False


class TriggerTask(Task):
    """Triggered Task.

    Args:
        function (callable): function to use for the main operation
        trigger (CronTrigger, DateTrigger, IntervalTrigger): trigger
            for task execution.
        broker (Broker): an Alsek broker
        name (str, optional): the name of the task. If ``None``,
            the class name will be used.
        queue (str, optional): the name of the queue to generate the task on.
            If ``None``, the default queue will be used.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff, optional): backoff algorithm and parameters to use when computing
            delay between retries
        result_store (ResultStore, optional): store for persisting task results
        status_tracker (StatusTracker, optional): store for persisting task statuses
        mechanism (SupportedMechanismType): mechanism for executing the task. Must
            be either "process" or "thread".
        no_positional_args (bool): if ``True``, the task will not accept positional arguments.

    Warnings:
        * The signature of ``function`` cannot contain parameters

    Raises:
        * ``SchedulingError``: if the signature of ``function`` includes
            parameters.

    """

    def __init__(
        self,
        function: Callable[..., Any],
        trigger: Union[CronTrigger, DateTrigger, IntervalTrigger],
        broker: Broker,
        name: Optional[str] = None,
        queue: Optional[str] = None,
        timeout: int = DEFAULT_TASK_TIMEOUT,
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        backoff: Optional[Backoff] = ExponentialBackoff(),
        result_store: Optional[ResultStore] = None,
        status_tracker: Optional[StatusTracker] = None,
        mechanism: SupportedMechanismType = DEFAULT_MECHANISM,
        no_positional_args: bool = False,
    ) -> None:
        if inspect.signature(function).parameters:
            raise SchedulingError("Function signature cannot includes parameters")
        super().__init__(
            function=function,
            broker=broker,
            name=name,
            queue=queue,
            timeout=timeout,
            max_retries=max_retries,
            backoff=backoff,
            result_store=result_store,
            status_tracker=status_tracker,
            mechanism=mechanism,
            no_positional_args=no_positional_args,
        )
        self.trigger = trigger

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def serialize(self) -> dict[str, Any]:
        serialized_task = super().serialize()
        serialized_task["settings"]["trigger"] = self.trigger
        return serialized_task

    @property
    def _job(self) -> Optional[Job]:
        return self.scheduler.get_job(self.name)

    def _submit(self, message: Message, **options: Any) -> None:
        if self._job:
            raise SchedulingError("Task already scheduled")

        self.scheduler.add_job(
            _MultiSubmit(
                message=message,
                broker=self.broker,
                on_submit=self.on_submit,
                callback_op=partial(self._update_status, status=TaskStatus.SUBMITTED),
                options=options,
            ),
            trigger=self.trigger,
            id=self.name,
        )

    @property
    def generated(self) -> bool:
        """If the task has been generated."""
        return bool(self._job)

    def clear(self) -> None:
        """Clear the currently scheduled task.

        Returns:
            None

        Raises
            * AttributeError: if a task has not yet
                been generated

        """
        try:
            self.scheduler.remove_job(self.name)
        except JobLookupError:
            raise AttributeError("Task not generated")

    def pause(self) -> None:
        """Pause the underlying scheduler.

        Returns:
            None

        """
        self.scheduler.pause()

    def resume(self) -> None:
        """Resume the underlying scheduler.

        Returns:
            None

        """
        self.scheduler.resume()

    def shutdown(self) -> None:
        """Shutdown the underlying scheduler.

        Returns:
            None

        """
        self.scheduler.shutdown()


def _parse_base_task(
    base_task: Optional[Type[Task]],
    trigger: Optional[Union[CronTrigger, DateTrigger, IntervalTrigger]],
) -> Type[Task]:
    if base_task:
        return base_task
    elif trigger:
        return TriggerTask
    else:
        return Task


def task(
    broker: Broker,
    name: Optional[str] = None,
    queue: Optional[str] = None,
    timeout: int = DEFAULT_TASK_TIMEOUT,
    max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
    backoff: Optional[Backoff] = ExponentialBackoff(),
    trigger: Optional[Union[CronTrigger, DateTrigger, IntervalTrigger]] = None,
    result_store: Optional[ResultStore] = None,
    status_tracker: Optional[StatusTracker] = None,
    mechanism: SupportedMechanismType = DEFAULT_MECHANISM,
    no_positional_args: bool = False,
    base_task: Optional[Type[Task]] = None,
) -> Callable[..., Task]:
    """Wrapper for task construction.

    Args:
        broker (Broker): an Alsek broker
        name (str, optional): the name of the task. If ``None``,
            the class name will be used.
        queue (str, optional): the name of the queue to generate the task on.
            If ``None``, the default queue will be used.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff, optional): backoff algorithm and parameters to use when computing
            delay between retries
        trigger (CronTrigger, DateTrigger, IntervalTrigger, optional): trigger
            for task execution.
        result_store (ResultStore, optional): store for persisting task results
        status_tracker (StatusTracker, optional): store for persisting task statuses
        mechanism (SupportedMechanismType): mechanism for executing the task. Must
            be either "process" or "thread".
        no_positional_args (bool): if ``True``, the task will not accept positional arguments.
        base_task (Type[Task]): base to use for task constuction.
            If ``None``, a base task will be selected automatically.

    Returns:
        wrapper (callable): task-wrapped function

    Raises:
        * ValueError: if a ``trigger`` and not supported by ``base_task``

    Examples:
        >>> from alsek import Broker, task
        >>> from alsek.storage.backends.disk import DiskCacheBackend

        >>> backend = DiskCacheBackend()
        >>> broker = Broker(backend)

        >>> @task(broker)
        ... def add(a: int, b: int) -> int:
        ...     return a + b

    """
    parsed_base_task = _parse_base_task(base_task, trigger=trigger)
    base_task_signature = inspect.signature(parsed_base_task.__init__)

    if trigger and "trigger" not in base_task_signature.parameters:
        raise ValueError(f"Trigger not supported by {parsed_base_task}")

    def wrapper(function: Callable[..., Any]) -> Task:
        return parsed_base_task(  # type: ignore
            function=function,
            name=name,
            broker=broker,
            queue=queue,
            timeout=timeout,
            max_retries=max_retries,
            backoff=backoff,
            mechanism=mechanism,
            no_positional_args=no_positional_args,
            result_store=result_store,
            status_tracker=status_tracker,
            **(  # noqa (handled above)
                dict(trigger=trigger)
                if "trigger" in base_task_signature.parameters
                else dict()
            ),
        )

    return wrapper
