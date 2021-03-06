"""

    Consumer

"""
from typing import Dict, Iterable, List, Optional, Union

from alsek._utils.system import StopSignalListener
from alsek.core.backoff import Backoff, ConstantBackoff, LinearBackoff
from alsek.core.broker import Broker
from alsek.core.concurrency import Lock
from alsek.core.message import Message
from alsek.storage.backends import Backend


class _ConsumptionMutex(Lock):
    def __init__(
        self,
        message: Message,
        backend: Backend,
        ttl_buffer: int = 90 * 1000,
    ) -> None:
        super().__init__(
            name=message.uuid,
            backend=backend,
            ttl=message.timeout + ttl_buffer,
            auto_release=False,
        )


class Consumer:
    """Tool for consuming messages generated by the broker.

    Args:
        broker (Broker): an Alsek broker
        subset (List[str], Dict[str, List[str]], optional): subset of messages to consume
            Must be one of the following

                * ``None``: consume messages from all queues and tasks
                * ``list``: a list of queues of the form ``["queue_a", "queue_b", "queue_c", ...]``
                * ``dict``: a dictionary of queues and tasks of the form
                    ``{"queue_a": ["task_name_a", "task_name_b", "task_name_c", ...], ...}``

        backoff (Backoff, optional): backoff to use in response to passes over the backend
            which did not yield any actionable messages.

    Notes:
        * If ``subset`` is a ``list`` or ``dict``, queue priority is derived from the
          order of the items. Items which appear earlier are given higher priority.
        * If ``subset`` is a ``dict``, task priority is derived from the order of
          task names in the value associated with each key (queue).

    Warning:
        * If ``subset`` is of type ``dict``, task names not included
          in the any of the values will be ignored.

    """

    def __init__(
        self,
        broker: Broker,
        subset: Optional[Union[List[str], Dict[str, List[str]]]] = None,
        backoff: Optional[Backoff] = LinearBackoff(
            1 * 1000,
            floor=1000,
            ceiling=30_000,
            zero_override=False,
        ),
    ) -> None:
        self.subset = subset
        self.broker = broker
        self.backoff = backoff or ConstantBackoff(0, floor=0, ceiling=0)

        self._empty_passes: int = 0
        self.stop_signal = StopSignalListener()

    def _scan_subnamespaces(self) -> Iterable[str]:
        if not self.subset:
            subnamespaces = [self.broker.get_subnamespace(None)]
        elif isinstance(self.subset, list):
            subnamespaces = [self.broker.get_subnamespace(q) for q in self.subset]
        else:
            subnamespaces = [
                self.broker.get_subnamespace(q, task_name=t)
                for (q, tasks) in self.subset.items()
                for t in tasks
            ]

        for s in subnamespaces:
            yield from self.broker.backend.scan(f"{s}*")

    def _poll(self) -> Iterable[Message]:
        empty: bool = True
        for name in self._scan_subnamespaces():
            message_data = self.broker.backend.get(name)
            if message_data is None:
                # Message data can be None if it is deleted (e.g., by
                # a TTL or worker) between the scan() and get() operations.
                continue

            message = Message(**message_data)
            if message.ready:
                empty = False
                with _ConsumptionMutex(message, self.broker.backend) as lock:
                    if lock.acquire(strict=False):
                        yield message._link_lock(lock, override=True)

        self._empty_passes = self._empty_passes + 1 if empty else 0

    def stream(self) -> Iterable[Message]:
        """Generate a stream of messages to process from
        the data backend.

        Returns:
            stream (Iterable[Message]): an iterable of messages to process

        """
        while not self.stop_signal.received:
            for message in self._poll():
                yield message
            self.stop_signal.wait(self.backoff.get(self._empty_passes))
