"""

    Task

"""
from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import dill
from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek._defaults import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_MECHANISM,
    DEFAULT_QUEUE,
    DEFAULT_TASK_TIMEOUT,
)
from alsek._utils.aggregation import gather_init_params
from alsek._utils.printing import auto_repr
from alsek.core.backoff import Backoff, ExponentialBackoff
from alsek.core.broker import Broker
from alsek.core.message import Message
from alsek.exceptions import SchedulingError, ValidationError
from alsek.storage.result import ResultStore

log = logging.getLogger(__name__)

SUPPORTED_MECHANISMS: Tuple[str, ...] = ("process", "thread")


def _expects_message(function: Callable[..., Any]) -> bool:
    parameters = inspect.signature(function).parameters
    if "message" in parameters:
        if parameters["message"].annotation != inspect._empty:  # type: ignore
            return bool(parameters["message"].annotation == Message)
        else:
            return True
    else:
        return False


def _collapse_callbacks(callbacks: Tuple[Message, ...]) -> Message:
    # Note: this function operates 'in place' on the items in
    # `callbacks`. This is done so that callback information
    # will become available on the original messages.
    if any(c.callback_message_data for c in callbacks):
        raise AttributeError(f"Existing callbacks in {callbacks}")

    for i in range(1, len(callbacks))[::-1]:
        callbacks[i - 1].update(callback_message_data=callbacks[i].data)
    return callbacks[0]


def _parse_callback(callback: Union[Message, Tuple[Message, ...]]) -> Message:
    if isinstance(callback, Message):
        return callback
    else:
        return _collapse_callbacks(callback)


class _MultiSubmit:
    def __init__(self, message: Message, broker: Broker, **options: Any) -> None:
        self._message = message
        self.broker = broker
        self.options = options

        self.first: bool = True

    @property
    def message(self) -> Message:
        return (
            self._message
            if self.first
            else self._message.duplicate().update(progenitor=self._message.uuid)
        )

    def __call__(self) -> None:
        self.broker.submit(self.message, **self.options)
        self.first = False


class Task:
    """Alsek Task.

    Args:
        function (callable): function to use for the main operation
        broker (Broker): an Alsek broker
        name (str, optional): the name of the task. If ``None``,
            the class name will be used.
        queue (str, optional): the name of the queue to generate the task on.
            If ``None``, the default queue will be used.
        priority (int): priority of the task. Tasks with lower values
            will be executed before tasks with higher values.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff): backoff algorithm and parameters to use when computing
            delay between retries
        result_store (ResultStore): store for persisting task results
        mechanism (str): mechanism for executing the task. Must
            be either "process" or "thread".

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
        priority: int = 0,
        timeout: int = DEFAULT_TASK_TIMEOUT,
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        backoff: Backoff = ExponentialBackoff(),
        result_store: Optional[ResultStore] = None,
        mechanism: str = DEFAULT_MECHANISM,
    ) -> None:
        self.function = function
        self.broker = broker
        self.queue = queue or DEFAULT_QUEUE
        self.priority = priority
        self.timeout = timeout
        self._name = name
        self.max_retries = max_retries
        self.backoff = backoff
        self.result_store = result_store
        self.mechanism = mechanism

        if priority < 0:
            raise ValueError("`priority` must be greater than or equal to zero")
        elif mechanism not in SUPPORTED_MECHANISMS:
            raise ValueError(f"Unsupported mechanism '{mechanism}'")

        self._deferred: bool = False

    def _serialize(self) -> Dict[str, Any]:
        settings = gather_init_params(self, ignore=("broker",))
        settings["broker"] = gather_init_params(self.broker, ignore=("backend",))
        settings["broker"]["backend"] = self.broker.backend._encode()
        return dict(task=self.__class__, settings=settings)

    @staticmethod
    def _deserialize(data: Dict[str, Any]) -> Task:
        def unwind_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
            backend_data = dill.loads(settings["broker"]["backend"])
            settings["broker"]["backend"] = backend_data["backend"]._from_settings(
                backend_data["settings"]
            )
            settings["broker"] = Broker(**settings["broker"])
            return settings

        return data["task"](**unwind_settings(data["settings"]))

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
            priority=self.priority,
            max_retries=self.max_retries,
            backoff=self.backoff,
            mechanism=self.mechanism,
            deferred_mode="enabled" if self._deferred else "disabled",
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)

    @property
    def deferred(self) -> bool:
        """Whether or not deferred mode is currently enabled."""
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

    def _submit(self, message: Message, **options: Any) -> None:
        self.broker.submit(message, **options)

    def _validate(self, store_result: Optional[bool]) -> None:
        if store_result and not self.result_store:
            raise ValidationError(f"Cannot store result, `result_store` not set")

    def generate(
        self,
        args: Optional[Union[List[Any], Tuple[Any, ...]]] = None,
        kwargs: Optional[Dict[Any, Any]] = None,
        metadata: Optional[Dict[Any, Any]] = None,
        store_result: Optional[bool] = None,
        result_ttl: Optional[int] = None,
        uuid: Optional[str] = None,
        timeout_override: Optional[int] = None,
        delay: Optional[int] = None,
        previous_result: Any = None,
        callback: Optional[Union[Message, Tuple[Message, ...]]] = None,
        submit: bool = True,
        **options: Any,
    ) -> Message:
        """Generate an instance of the task for processing.

        This method generates a new message for the task and
        submit it to the broker.

        Args:
            args (list, tuple, optional): positional arguments to pass to ``function``
            kwargs (dict, optional): keyword arguments to pass to ``function``
            metadata (dict, optional): a dictionary of user-defined message metadata.
                This can store any data types supported by the backend's serializer.
            store_result (bool, optional): if ``True`` persist the result to a result store.
                If ``None``, this method will default to ``True`` if ``ResultStore`` has been
                set for the task, and ``False`` otherwise.
            result_ttl (int, optional): time to live (in milliseconds) for the
                result in the result store. If ``None``, the result will be
                persisted indefinitely. Note that if ``store_result`` is ``False``
                this parameter will be ignored.
            uuid (str, optional): universal unique identifier for the message.
                If ``None``, one will be generated automatically.
            timeout_override (int, optional): override the default maximum runtime
                (in milliseconds) for instances of this task.
            set maximum amount of time (in milliseconds)
                this message is permitted to run. This will override the default
                for the task.
            delay (int, optional): delay before message is ready
            previous_result (Any): result of a previous task.
            callback (Message, Tuple[Message, ...], optional): one ore more messages
                to be submitted to the broker after the proceeding message has been
                successfully processed by a worker.
            submit (bool): if ``True`` submit the task to the broker
            options (Keyword Args): options to use when submitting
                the message via the broker. See ``Broker.submit()``.

        Returns:
            message (Message): message generated for the task

        Warning:
            * ``submit`` is overridden to ``False`` if deferred mode is active
            * ``uuid`` is refreshed after the first event when using a trigger.

        """
        self._validate(store_result)

        message = Message(
            task_name=self.name,
            queue=self.queue,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
            store_result=(
                bool(self.result_store) if store_result is None else store_result
            ),
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

    def execute(self, message: Message) -> Any:
        """Execute the task against a message.

        Args:
            message (Message): message to process

        Returns:
            result (Any): output of ``op()``

        """
        self.pre_op(message)
        result = self.op(message)
        self.post_op(message, result=result)
        return result

    def do_retry(self, message: Message, exception: BaseException) -> bool:  # noqa
        """Whether or not a failed task should be retried.

        Args:
            message (Message): message which failed
            exception (BaseException): the exception which was raised

        Returns:
            bool

        """
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
              this method will *not* be invoked.

        """
        return True


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
        priority (int): priority of the task. Tasks with lower values
            will be executed before tasks with higher values.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff): backoff algorithm and parameters to use when computing
            delay between retries
        result_store (ResultStore): store for persisting task results
        mechanism (str): mechanism for executing the task. Must
            be either "process" or "thread".

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
        priority: int = 0,
        timeout: int = DEFAULT_TASK_TIMEOUT,
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        backoff: Backoff = ExponentialBackoff(),
        result_store: Optional[ResultStore] = None,
        mechanism: str = DEFAULT_MECHANISM,
    ) -> None:
        if inspect.signature(function).parameters:
            raise SchedulingError("Function signature cannot includes parameters")
        super().__init__(
            function=function,
            broker=broker,
            name=name,
            queue=queue,
            priority=priority,
            timeout=timeout,
            max_retries=max_retries,
            backoff=backoff,
            result_store=result_store,
            mechanism=mechanism,
        )
        self.trigger = trigger

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def _serialize(self) -> Dict[str, Any]:
        serializable_task = super()._serialize()
        serializable_task["settings"]["trigger"] = self.trigger
        return serializable_task

    @property
    def _job(self) -> Optional[Job]:
        return self.scheduler.get_job(self.name)

    def _submit(self, message: Message, **options: Any) -> None:
        if self._job:
            raise SchedulingError("Task already scheduled")

        self.scheduler.add_job(
            _MultiSubmit(message, broker=self.broker, options=options),
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
    priority: int = 0,
    timeout: int = DEFAULT_TASK_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff: Backoff = ExponentialBackoff(),
    trigger: Optional[Union[CronTrigger, DateTrigger, IntervalTrigger]] = None,
    result_store: Optional[ResultStore] = None,
    mechanism: str = DEFAULT_MECHANISM,
    base_task: Optional[Type[Task]] = None,
) -> Callable[..., Task]:
    """Wrapper for task construction.

    Args:
        broker (Broker): an Alsek broker
        name (str, optional): the name of the task. If ``None``,
            the class name will be used.
        queue (str, optional): the name of the queue to generate the task on.
            If ``None``, the default queue will be used.
        priority (int): priority of the task. Tasks with lower values
            will be executed before tasks with higher values.
        timeout (int): the maximum amount of time (in milliseconds)
            this task is permitted to run.
        max_retries (int, optional): maximum number of allowed retries
        backoff (Backoff): backoff algorithm and parameters to use when computing
            delay between retries
        trigger (CronTrigger, DateTrigger, IntervalTrigger, optional): trigger
            for task execution.
        result_store (ResultStore): store for persisting task results
        mechanism (str): mechanism for executing the task. Must
            be either "process" or "thread".
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
            priority=priority,
            timeout=timeout,
            max_retries=max_retries,
            backoff=backoff,
            mechanism=mechanism,
            result_store=result_store,
            **(  # noqa (handled above)
                dict(trigger=trigger)
                if trigger in base_task_signature.parameters
                else dict()
            ),
        )

    return wrapper
