"""

    Result Storage

"""
from datetime import datetime
from typing import Any, Iterable, List, Union

from alsek._utils.waiting import waiter
from alsek.core.message import Message
from alsek.storage.backends import Backend

_GET_RESULT_WAIT_SLEEP_INTERVAL: int = 500


class ResultStore:
    """Alsek Result Store.

    Args:
        backend (Backend): backend for data storage

    Warning:
        * In order for a result to be stored, it must be
          serializable by the ``serializer`` used by ``backend``.

    """

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    @staticmethod
    def _get_stable_prefix(message: Message) -> str:
        """Get a prefix that does not change based on
        whether or not the message has a progenitor or not."""
        return f"results:{message.progenitor if message.progenitor else message.uuid}"

    @staticmethod
    def get_storage_name(message: Message) -> str:
        """Get the name for ``message`` in the backend.

        Args:
            message (Message): an Alsek message

        Returns:
            name (str): message-specific name

        """
        if message.progenitor:
            return f"results:{message.progenitor}:descendants:{message.uuid}"
        else:
            return f"results:{message.uuid}"

    def exists(self, message: Message) -> bool:
        """Whether or not data for ``message`` exists in the store.

        Args:
            message (Message): an Alsek message

        Returns:

        """
        return self.backend.exists(self.get_storage_name(message))

    def set(self, message: Message, result: Any, nx: bool = True) -> None:
        """Store a ``result`` for ``message``.

        Args:
            message (Message): an Alsek message.
            result (Any): the result to persist
            nx (bool): whether or not the item must not exist prior to being set

        Returns:
            None

        """
        self.backend.set(
            self.get_storage_name(message),
            value={"result": result, "timestamp": datetime.utcnow().timestamp()},
            nx=nx,
            ttl=message.result_ttl,
        )

    def _multi_get(self, names: Iterable[str], with_timestamp: bool) -> List[Any]:
        raw_results = sorted(
            [self.backend.get(n) for n in names],
            key=lambda d: d["timestamp"],  # type: ignore
        )
        return raw_results if with_timestamp else [r["result"] for r in raw_results]

    def get(
        self,
        message: Message,
        timeout: int = 0,
        keep: bool = False,
        with_timestamp: bool = False,
        descendants: bool = False,
    ) -> Union[Any, List[Any]]:
        """Get the result for ``message``.

        Args:
            message (Message): an Alsek message.
            timeout (int): amount of time (in milliseconds) to wait
                for the result to become available
            keep (bool): whether or not to keep the result afer fetching it.
                Defaults to ``False`` to conserve storage space.
            with_timestamp (bool): if ``True`` return results of the form
                ``{"result": <result>, "timestamp": int}``, where "timestamp"
                is the time at which the result was written to the backend.
            descendants (bool): if ``True`` fetch any decendant results.

        Returns:
            result (Any, List[Any]): the stored result. If ``descendants``
                is ``True`` a list of results will be returned.

        Notes:
            * The order of results when ``descendants=True`` is determined
              by the time at which the data was written to the backend.
            * ``timeout`` only applies to ``message``, even if ``descendants=True``.

        Warning:
            * If a message has a projenitor, the ``projenitor`` field in the
              ``message`` must be set.

        Examples:
            >>> from alsek.core import Message
            >>> from alsek.storage.backends.disk import DiskCacheBackend
            >>> from alsek.storage.result import ResultStore

            >>> backend = DiskCacheBackend()
            >>> result_store = ResultStore(backend)

            >>> result_store.get(Message(uuid="..."))

        """
        if timeout:
            waiter(
                lambda: self.exists(message),
                timeout=timeout,
                timeout_msg=f"Timeout waiting on result for {message.summary}",
                sleep_interval=_GET_RESULT_WAIT_SLEEP_INTERVAL,
            )

        if descendants:
            names = list(self.backend.scan(f"{self._get_stable_prefix(message)}*"))
        else:
            names = [self.get_storage_name(message)]

        results = self._multi_get(names, with_timestamp=with_timestamp)
        if not keep:
            for n in names:
                self.backend.delete(n)

        return results if descendants else results[0]

    def delete(self, message: Message) -> None:
        """Delete any data for ``message`` from the backend.

        Args:
            message (Message): an Alsek message.

        Returns:
            None

        """
        self.backend.delete(self.get_storage_name(message), missing_ok=True)
