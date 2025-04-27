"""

    Process Worker Pool

"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

from alsek import Message
from alsek.core.futures import ProcessTaskFuture
from alsek.core.worker._base import BaseWorkerPool
from alsek.exceptions import TerminationError
from alsek.utils.system import smart_cpu_count

log = logging.getLogger(__name__)


class ProcessWorkerPool(BaseWorkerPool):
    """Fixed-size pool that runs each task in its *own* forked process
    (via `ProcessTaskFuture`).

    Args:
        n_processes (int): Maximum number of live `ProcessTaskFuture`s.
        prune_interval (int): Number of milliseconds between background
            runs of a scan to prune spent futures.

    """

    def __init__(
        self,
        n_processes: Optional[int] = None,
        prune_interval: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(mechanism="process", **kwargs)
        self.n_processes = n_processes or smart_cpu_count()

        self._futures: List[ProcessTaskFuture] = list()

        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.scheduler.add_job(
            self.prune,
            trigger="interval",
            seconds=prune_interval / 1000,
            id="prune_scan",
        )

    def on_boot(self) -> None:
        log.info(
            "Starting process-based worker pool with up to %s workers (%s max process%s)...",
            self.n_processes,
            self.n_processes,
            "" if self.n_processes == 1 else "es",
        )
        super().on_boot()

    def has_slot(self) -> bool:
        return len(self._futures) < self.n_processes

    def prune(self) -> None:
        """Prune spent futures."""
        kept: list[ProcessTaskFuture] = list()
        for f in self._futures:
            if f.time_limit_exceeded:
                f.stop(TimeoutError)
                f.clean_up(ignore_errors=True)
            elif not f.complete:
                kept.append(f)
        self._futures = kept

    def on_shutdown(self) -> None:
        """Terminate everything that is still alive."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

        for f in self._futures:
            if not f.complete:
                f.stop(TerminationError)
                f.clean_up(ignore_errors=True)
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
