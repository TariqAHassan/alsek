"""

    Result Iteration

"""
from typing import Any, Iterable, List, Tuple

from alsek._utils.system import StopSignalListener
from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.storage.result import ResultStore


def _multi_pop(messages: List[Message], to_pop: List[int]) -> List[Message]:
    for i in to_pop:
        messages.pop(i)
    return messages


def _duplicates(items: Iterable[Any]) -> bool:
    return len(set(items)) != len(items)


class ResultPool:
    """Tooling for iterating over task results.

    Args:
        result_store (ResultStore): store where task results are persisted

    Examples:
        >>> from alsek.storage.result import ResultStore
        >>> from alsek.tools.iteration import ResultPool
        ...
        >>> pool = ResultPool()
        ...
        >>> messages = [...]
        >>> for uuid, result in pool.istream(*messages):
        ...     pass

    """

    def __init__(self, result_store: ResultStore) -> None:
        self.result_store = result_store

        self.stop_signal = StopSignalListener()

    @staticmethod
    def _validate(messages: List[Message]) -> None:
        if _duplicates([m.uuid for m in messages]):
            raise ValidationError("Duplicate messages detected")
        elif not all(m.store_result for m in messages):
            raise ValidationError("Messages without result storage detected")

    def istream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        **kwargs: Any,
    ) -> Iterable[Tuple[str, Any]]:
        """Stream the results of one or more messages. Results are yielded
        in the order in which they become available. (This may differ from
        the order in which messages are provided.)

        Args:
            *messages (Message): one or more messages to iterate over
            wait (int): time to wait (in milliseconds) between checks for
                available results
            **kwargs (Keyword Args): keyword arguments to pass to
                ``result_store.get()``.

        results (iterable): an iterable of results of the form
            ``("uuid", result)``.

        """
        self._validate(messages)
        messages = list(messages)
        while messages and not self.stop_signal.received:
            to_pop = list()
            for e, m in enumerate(messages):
                try:
                    yield m.uuid, self.result_store.get(m, **kwargs)
                    to_pop.append(e)
                except KeyError:
                    pass

            messages = _multi_pop(messages, to_pop=to_pop)
            self.stop_signal.wait(wait if messages else 0)

    def stream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        **kwargs: Any,
    ) -> Iterable[Tuple[str, Any]]:
        """Stream the results of one or more messages. The order of the
        results are guaranteed to match the order of ``messages``.

        Args:
            *messages (Message): one or more messages to iterate over
            wait (int): time to wait (in milliseconds) between checks for
                available results
            **kwargs (Keyword Args): keyword arguments to pass to
                ``result_store.get()``.

        Returns:
            results (iterable): an iterable of results of the form
                ``("uuid", result)``.

        """
        order = {m.uuid: e for e, m in enumerate(messages)}
        results = self.istream(*messages, wait=wait, **kwargs)
        yield from sorted(results, key=lambda x: order.get(x[0]))
