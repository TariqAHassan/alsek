"""

    Result Iteration

"""
from typing import Any, Iterable, List, Set, Tuple

from alsek._utils.checks import has_duplicates
from alsek._utils.system import StopSignalListener
from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.storage.result import ResultStore


def _idx_drop(items: List[Any], indexes: Set[int]) -> List[Any]:
    return [i for e, i in enumerate(items) if e not in indexes]


class ResultPool:
    """Tooling for iterating over task results.

    Args:
        result_store (ResultStore): store where task results are persisted

    Examples:
        >>> from alsek.storage.result import ResultStore
        >>> from alsek.tools.iteration import ResultPool
        ...
        >>> result_store = ResultStore(...)
        >>> pool = ResultPool(result_store)
        ...
        >>> messages = [...]
        >>> for uuid, result in pool.istream(*messages):
        ...     pass

    """

    def __init__(self, result_store: ResultStore) -> None:
        self.result_store = result_store

        self.stop_signal = StopSignalListener()

    @staticmethod
    def _validate(messages: Tuple[Message, ...]) -> None:
        if has_duplicates([m.uuid for m in messages]):
            raise ValidationError("Duplicate messages detected")

    def _engine(
        self,
        messages: Tuple[Message, ...],
        wait: int,
        break_on_key_error: bool,
        **kwargs: Any,
    ) -> Iterable[Tuple[Message, Any]]:
        self._validate(messages)

        outstanding = list(range(len(messages)))
        while outstanding and not self.stop_signal.received:
            to_drop = set()
            for i in outstanding:
                try:
                    yield messages[i], self.result_store.get(messages[i], **kwargs)
                    to_drop.add(i)
                except KeyError:
                    if break_on_key_error:
                        break

            outstanding = _idx_drop(outstanding, indexes=to_drop)
            self.stop_signal.wait(wait if outstanding else 0)

    def istream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        **kwargs: Any,
    ) -> Iterable[Tuple[Message, Any]]:
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
            ``(Message, result)``.

        Warning:
            * By default, ``result_store`` does not keep messages once
              they have been collected. As a result, providing messages
              for which the corresponding results have already been collected
              (and deleted) will cause this method to loop indefinitely.
              In order to loop over messages multiple times set ``keep=True``.

        """
        yield from self._engine(messages, wait=wait, break_on_key_error=False, **kwargs)

    def stream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        **kwargs: Any,
    ) -> Iterable[Tuple[Message, Any]]:
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
                ``(Message, result)``.

        Warning:
            * By default, ``result_store`` does not keep messages once
              they have been collected. As a result, providing messages
              for which the corresponding results have already been collected
              (and deleted) will cause this method to loop indefinitely.
              In order to loop over messages multiple times set ``keep=True``.

        """
        yield from self._engine(messages, wait=wait, break_on_key_error=True, **kwargs)
