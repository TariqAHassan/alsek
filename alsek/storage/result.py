"""

    Result Storage

"""
from typing import Any, Dict, Iterable, List, Union

from alsek._utils.temporal import utcnow_timestamp_ms
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

    @staticmethod
    def _extract_uuid(storage_name: str) -> str:
        return storage_name.rsplit(":", 1)[-1]

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
            nx (bool): if ``True`` the item must not exist prior to being set

        Returns:
            None

        """
        self.backend.set(
            self.get_storage_name(message),
            value={"result": result, "timestamp": utcnow_timestamp_ms()},
            nx=nx,
            ttl=message.result_ttl,
        )

    def _determine_names(self, message: Message, descendants: bool) -> List[str]:
        if descendants:
            return list(self.backend.scan(f"{self._get_stable_prefix(message)}*"))
        else:
            return [self.get_storage_name(message)]

    def _get_engine(self, names: Iterable[str], with_metadata: bool) -> List[Any]:
        def bundle_data(n: str) -> Dict[str, Any]:
            data: Dict[str, Any] = self.backend.get(n)
            if with_metadata:
                data["uuid"] = self._extract_uuid(n)
            return data

        results = sorted(
            [bundle_data(n) for n in names],
            key=lambda d: d["timestamp"],  # type: ignore
        )
        return results if with_metadata else [r["result"] for r in results]

    def get(
        self,
        message: Message,
        timeout: int = 0,
        keep: bool = False,
        with_metadata: bool = False,
        descendants: bool = False,
    ) -> Union[Any, List[Any]]:
        """Get the result for ``message``.

        Args:
            message (Message): an Alsek message.
            timeout (int): amount of time (in milliseconds) to wait
                for the result to become available
            keep (bool): whether or not to keep the result afer fetching it.
                Defaults to ``False`` to conserve storage space.
            with_metadata (bool): if ``True`` return results of the form
                ``{"result": <result>, "uuid": str, "timestamp": int}``, where
                "result" is the result persisted to the backend, "uuid" if the uuid
                 of the message associated with the result and "timestamp" is the
                time at which the result was written to the backend.
            descendants (bool): if ``True`` fetch any descendant results.

        Returns:
            result (Any, List[Any]): the stored result. If ``descendants``
                is ``True`` a list of results will be returned.

        Raises:
            KeyError: if results are not available for ``message``

        Notes:
            * The order of results when ``descendants=True`` is determined
              by the time at which the data was written to the backend.
            * ``timeout`` only applies to ``message``, even if ``descendants=True``.

        Warning:
            * If a message has a projenitor, the ``projenitor`` field in the
              ``message`` must be set.

        Examples:
            >>> from alsek import Message
            >>> from alsek.storage.backends.disk import DiskCacheBackend
            >>> from alsek.storage.result import ResultStore

            >>> backend = DiskCacheBackend()
            >>> result_store = ResultStore(backend)

            >>> result_store.get(Message(uuid="..."))

        """
        if not self.exists(message):
            if timeout:
                waiter(
                    lambda: self.exists(message),
                    timeout=timeout,
                    timeout_msg=f"Timeout waiting on result for {message.summary}",
                    sleep_interval=_GET_RESULT_WAIT_SLEEP_INTERVAL,
                )
            else:
                raise KeyError(f"No results for {message.uuid}")

        names = self._determine_names(message, descendants=descendants)
        results = self._get_engine(names, with_metadata=with_metadata)
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
