"""

    Process Worker Pool

"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from alsek import Message
from alsek.core.futures import ProcessTaskFuture
from alsek.core.pools._base import BaseWorkerPool
from alsek.exceptions import TerminationError
from alsek.utils.system import smart_cpu_count

log = logging.getLogger(__name__)


class ProcessWorkerPool(BaseWorkerPool):
    """Fixed-size pool that runs each task in its *own* forked process
    (via `ProcessTaskFuture`).

    Args:
        n_processes (int): Maximum number of live `ProcessTaskFuture`s.

    """

    def __init__(self, n_processes: Optional[int] = None, **kwargs: Any) -> None:
        super().__init__(mechanism="process", **kwargs)
        self.n_processes = n_processes or smart_cpu_count()

        self._futures: List[ProcessTaskFuture] = list()

    def has_slot(self) -> bool:
        return len(self._futures) < self.n_processes

    def prune(self) -> None:
        """Prune spent futures."""
        kept: list[ProcessTaskFuture] = list()
        for fut in self._futures:
            if fut.time_limit_exceeded:
                fut.stop(TimeoutError)
                fut.clean_up(ignore_errors=True)
            elif not fut.complete:
                kept.append(fut)
        self._futures = kept

    def stop_all_futures(self) -> None:
        """Terminate everything that is still alive."""
        for fut in self._futures:
            fut.stop(TerminationError)
            fut.clean_up(ignore_errors=True)
        self._futures.clear()

    def submit_message(self, message: Message) -> bool:
        """Submit a single message"""
        submitted = False
        if self.has_slot():
            self._futures.append(
                ProcessTaskFuture(
                    task=self._task_map[message.task_name],
                    message=message,
                )
            )
            submitted = True
        return submitted
