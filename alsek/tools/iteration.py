"""

    Result Iteration

"""

from typing import Any, Iterable

from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.storage.result import ResultStore
from alsek.utils.checks import has_duplicates
from alsek.utils.system import StopSignalListener


def _idx_drop(items: list[Any], indexes: set[int]) -> list[Any]:
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

        self.stop_signal = StopSignalListener(exit_override=False)

    @staticmethod
    def _validate(messages: tuple[Message, ...]) -> None:
        if has_duplicates([m.uuid for m in messages]):
            raise ValidationError("Duplicate messages detected")

    def _engine(
        self,
        messages: tuple[Message, ...],
        wait: int,
        break_on_error: bool,
        **kwargs: Any,
    ) -> Iterable[tuple[Message, Any]]:
        self._validate(messages)

        outstanding = list(range(len(messages)))
        while outstanding and not self.stop_signal.received:
            to_drop = set()
            for i in outstanding:
                try:
                    yield messages[i], self.result_store.get(messages[i], **kwargs)
                    to_drop.add(i)
                except (KeyError, TimeoutError):
                    if break_on_error:
                        break

            outstanding = _idx_drop(outstanding, indexes=to_drop)
            self.stop_signal.wait(wait if outstanding else 0)

    def istream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        descendants: bool = False,
        **kwargs: Any,
    ) -> Iterable[tuple[Message, Any]]:
        """Stream the results of one or more messages. Results are yielded
        in the order in which they become available. (This may differ from
        the order in which messages are provided.)

        Args:
            *messages (Message): one or more messages to iterate over
            wait (int): time to wait (in milliseconds) between checks for
                available results
            descendants (bool): if ``True``, wait for and return
                the results of all descendant (callback) messages.
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
        yield from self._engine(
            messages,
            wait=wait,
            break_on_error=False,
            descendants=descendants,
            **kwargs,
        )

    def stream(
        self,
        *messages: Message,
        wait: int = 5 * 1000,
        descendants: bool = False,
        **kwargs: Any,
    ) -> Iterable[tuple[Message, Any]]:
        """Stream the results of one or more messages. The order of the
        results are guaranteed to match the order of ``messages``.

        Args:
            *messages (Message): one or more messages to iterate over
            wait (int): time to wait (in milliseconds) between checks for
                available results
            descendants (bool): if ``True``, wait for and return
                the results of all descendant (callback) messages.
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
        yield from self._engine(
            messages,
            wait=wait,
            break_on_error=True,
            descendants=descendants,
            **kwargs,
        )
