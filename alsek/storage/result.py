"""

    Result Storage

"""

from __future__ import annotations

from typing import Any, Iterable, Union

import dill

from alsek.core.message import Message
from alsek.storage.backends import Backend
from alsek.utils.namespacing import get_result_name, get_stable_result_prefix
from alsek.utils.temporal import utcnow_timestamp_ms
from alsek.utils.waiting import waiter

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

    def serialize(self) -> dict[str, Any]:
        return {
            "backend": self.backend.encode(),
        }

    @staticmethod
    def deserialize(data: dict[str, Any]) -> ResultStore:
        backend_data = dill.loads(data["backend"])
        backend = backend_data["backend"]._from_settings(backend_data["settings"])
        return ResultStore(
            backend=backend,
        )

    def _get_all_storage_names(self, message: Message, descendants: bool) -> list[str]:
        if descendants:
            if message.descendant_uuids:
                descendant_names = [
                    get_result_name(
                        Message(message.task_name, uuid=u, progenitor_uuid=message.uuid)
                    )
                    for u in message.descendant_uuids
                ]
                return [*descendant_names, get_result_name(message)]
            else:
                return sorted(
                    self.backend.scan(f"{get_stable_result_prefix(message)}*"),
                    key=lambda i: 1 if i == message.uuid else 0,
                )
        else:
            return [get_result_name(message)]

    @staticmethod
    def _extract_uuid(storage_name: str) -> str:
        return storage_name.rsplit(":", 1)[-1]

    def exists(self, message: Message, descendants: bool = False) -> bool:
        """Whether data for ``message`` exists in the store.

        Args:
            message (Message): an Alsek message
            descendants (bool): if ``True``, this method will return ``True``
                iff all of the descendants of ``message`` also exist.

        Returns:
            bool

        """
        names = self._get_all_storage_names(message, descendants=descendants)
        return all(self.backend.exists(n) for n in names)

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
            get_result_name(message),
            value={"result": result, "timestamp": utcnow_timestamp_ms()},
            nx=nx,
            ttl=message.result_ttl,
        )

    def _get_engine(self, names: Iterable[str], with_metadata: bool) -> list[Any]:
        def bundle_data(n: str) -> dict[str, Any]:
            data: dict[str, Any] = self.backend.get(n)
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
    ) -> Union[Any, list[Any]]:
        """Get the result for ``message``.

        Args:
            message (Message): an Alsek message.
            timeout (int): amount of time (in milliseconds) to wait
                for the result to become available
            keep (bool): whether to keep the result after fetching it.
                Defaults to ``False`` to conserve storage space.
            with_metadata (bool): if ``True`` return results of the form
                ``{"result": <result>, "uuid": str, "timestamp": int}``, where
                "result" is the result persisted to the backend, "uuid" if the uuid
                 of the message associated with the result and "timestamp" is the
                time at which the result was written to the backend.
            descendants (bool): if ``True`` also fetch results for descendants.

        Returns:
            result (Any, list[Any]): the stored result. If ``descendants``
                is ``True`` a list of results will be returned.

        Raises:
            KeyError: if results are not available for ``message``
            TimeoutError: if results are not available for ``message``
                following ``timeout``.

        Notes:
            * The order of results when ``descendants=True`` is determined
              by the time at which the data was written to the backend.

        Warning:
            * If a message has a projenitor, the ``projenitor_uuid`` field in the
              ``message`` must be set.

        Examples:
            >>> from alsek import Message
            >>> from alsek.storage.backends.redis.standard import RedisBackend
            >>> from alsek.storage.result import ResultStore

            >>> backend = RedisBackend()
            >>> result_store = ResultStore(backend)

            >>> result_store.get(Message(uuid="..."))

        """
        if not self.exists(message, descendants=descendants):
            if timeout:
                waiter(
                    lambda: self.exists(message, descendants=descendants),
                    timeout=timeout,
                    timeout_msg=f"Timeout waiting on result for {message.summary}",
                    sleep_interval=_GET_RESULT_WAIT_SLEEP_INTERVAL,
                )
            else:
                raise KeyError(f"No results for {message.uuid}")

        names = self._get_all_storage_names(message, descendants=descendants)
        results = self._get_engine(names, with_metadata=with_metadata)
        if not keep:
            for n in names:
                self.backend.delete(n)

        return results if descendants else results[0]

    def delete(
        self,
        message: Message,
        descendants: bool = True,
        missing_ok: bool = False,
    ) -> int:
        """Delete any data for ``message`` from the backend.

        Args:
            message (Message): an Alsek message.
            descendants (bool): if ``True`` also delete results for descendants.
            missing_ok (bool): if ``True``, do not raise for missing

        Returns:
            count (int): number of results deleted

        """
        count: int = 0
        for name in self._get_all_storage_names(message, descendants=descendants):
            self.backend.delete(name, missing_ok=missing_ok)
            count += 1
        return count
