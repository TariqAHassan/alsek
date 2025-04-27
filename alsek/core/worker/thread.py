"""

    Thread Worker Pool

"""

from __future__ import annotations

import logging
import queue
from builtins import TimeoutError
from typing import Any, List, Optional

import dill

from alsek import Message
from alsek.core.futures import Event, Process, Queue, ThreadTaskFuture
from alsek.core.worker._base import BaseWorkerPool
from alsek.core.task import Task
from alsek.exceptions import TerminationError
from alsek.utils.system import smart_cpu_count

log = logging.getLogger(__name__)


class ThreadInProcessGroup:
    """
    • Runs inside a forked process
    • Accepts work items via `Queue`
    • Spawns at most `n_threads` ThreadTaskFutures concurrently
    """

    def __init__(
        self,
        q: Queue,
        shutdown_event: Event,
        n_threads: int,
        slot_wait_interval_seconds: float,
    ) -> None:
        self.q = q
        self.shutdown_event = shutdown_event
        self.n_threads = n_threads
        self.slot_wait_interval_seconds = slot_wait_interval_seconds

        self._live: list[ThreadTaskFuture] = list()

    def _prune(self) -> None:
        kept: list[ThreadTaskFuture] = list()
        for f in self._live:
            if f.time_limit_exceeded:
                f.stop(TimeoutError)
                f.clean_up(ignore_errors=True)
            elif not f.complete:
                kept.append(f)
        self._live = kept

    def _has_capacity(self) -> bool:
        return len(self._live) < self.n_threads

    def _spawn_future(self, payload: bytes) -> None:
        task_dict, msg_dict, exit_flag = dill.loads(payload)
        self._live.append(
            ThreadTaskFuture(
                task=Task.deserialize(task_dict),
                message=Message(**msg_dict),
                complete_only_on_thread_exit=exit_flag,
            )
        )

    def _stop_all_live_futures(self) -> None:
        for f in self._live:
            if not f.complete:
                f.stop(TerminationError)
                f.clean_up(ignore_errors=True)

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            # 1. reap finished / timed-out futures
            self._prune()

            # 2. Throttle if thread slots are full
            if not self._has_capacity():
                # Wait *either* for a slot OR the shutdown flag
                self.shutdown_event.wait(self.slot_wait_interval_seconds)
                continue

            # 3. Try to pull one unit of work
            try:
                payload = self.q.get(timeout=self.slot_wait_interval_seconds)
            except queue.Empty:
                continue

            # 4. Launch a new ThreadTaskFuture
            self._spawn_future(payload)

        self._stop_all_live_futures()


def _start_thread_worker(
    q: Queue,
    shutdown_event: Event,
    n_threads: int,
    slot_wait_interval_seconds: float,
) -> None:
    # We construct the worker *inside* the child so we don’t have to pickle it.
    worker = ThreadInProcessGroup(
        q=q,
        shutdown_event=shutdown_event,
        n_threads=n_threads,
        slot_wait_interval_seconds=slot_wait_interval_seconds,
    )
    worker.run()


class ProcessGroup:
    def __init__(
        self,
        n_threads: int,
        complete_only_on_thread_exit: bool,
        slot_wait_interval_seconds: float,
    ) -> None:
        self._n_threads = n_threads
        self.complete_only_on_thread_exit = complete_only_on_thread_exit
        self.slot_wait_interval_seconds = slot_wait_interval_seconds

        self.queue: Queue = Queue(maxsize=n_threads)
        self.shutdown_event: Event = Event()
        self.process = Process(
            target=_start_thread_worker,
            args=(
                self.queue,
                self.shutdown_event,
                n_threads,
                slot_wait_interval_seconds,
            ),
            daemon=True,
        )
        self.process.start()

    def has_slot(self) -> bool:
        return not self.queue.full()

    def submit(self, task: Task, message: Message) -> bool:
        payload = (
            task.serialize(),
            message.data,
            self.complete_only_on_thread_exit,
        )
        try:
            self.queue.put(dill.dumps(payload), block=False)
            return True
        except queue.Full:
            return False

    def stop(self, timeout: int | float = 2) -> None:
        """Stop the group of threads in this process group.

        Args:
            timeout (int, float): the time to wait in seconds

        Returns:
            None

        """
        # 1. Signal
        self.shutdown_event.set()
        # 2. Wait a bit for graceful exit
        self.process.join(timeout=timeout)
        # 3. Hard kill if still alive
        if self.process.is_alive():
            self.process.kill()


class ThreadWorkerPool(BaseWorkerPool):
    """Elastic thread-based pool.

    Args:
        n_threads (int): the number of threads to use per group.
        n_processes (int, optional): the number of process groups to use
        complete_only_on_thread_exit (bool): if ``True``, only mark the future
            as complete when the thread formally exits (i.e., is not alive).
            Pro: more rigorous — avoids marking the task complete until the thread fully terminates.
            Useful when you need strict control over thread lifecycle (e.g., for resource management).
            Con: may lead to hanging if the thread doesn’t terminate quickly (e.g., when using
            `thread_raise()` during revocation). Can also temporarily result in more than the
            allotted number of threads running, since a future is only removed from the pool
            after the thread is confirmed dead.
        slot_wait_interval_seconds (int): Number of milliseconds to wait when the
            pool is saturated before giving other workers a chance and re-scanning
            the queues.
        **kwargs (Keyword Args): Keyword arguments to pass to ``BaseWorkerPool()``.

    Notes:
        * Spawns a new **process** (ThreadProcessGroup) only when all existing
          groups are saturated and the hard ceiling `n_processes` hasn’t been hit.
        * Each group runs up to `n_threads` true ThreadTaskFutures concurrently.
        * Total worker capacity is ``n_threads * n_processes``.

    """

    def __init__(
        self,
        n_threads: int = 8,
        n_processes: Optional[int] = None,
        complete_only_on_thread_exit: bool = False,
        slot_wait_interval: int = 50,
        **kwargs: Any,
    ) -> None:
        super().__init__(mechanism="thread", **kwargs)
        self.n_threads = n_threads
        self.n_processes = n_processes or smart_cpu_count()
        self.complete_only_on_thread_exit = complete_only_on_thread_exit
        self.slot_wait_interval = slot_wait_interval

        self._progress_groups: List[ProcessGroup] = list()

    def on_boot(self) -> None:
        log.info(
            "Starting thread-based worker pool with up to %s workers (%s max thread%s and %s max process%s)...",
            self.n_threads * self.n_processes,
            self.n_threads,
            "" if self.n_threads == 1 else "es",
            self.n_processes,
            "" if self.n_processes == 1 else "es",
        )
        super().on_boot()

    def on_shutdown(self) -> None:
        """Stop all futures in the pool."""
        for g in self._progress_groups:
            g.stop()

    def prune(self) -> None:
        """Prune spent futures."""
        updated_groups = list()
        for g in self._progress_groups:
            g.process.join(timeout=0)  # reap quickly if already exited
            if g.process.is_alive():
                updated_groups.append(g)
        self._progress_groups = updated_groups

    def _acquire_group(self) -> Optional[ProcessGroup]:
        for g in self._progress_groups:
            if g.has_slot():
                return g

        if len(self._progress_groups) < self.n_processes:
            new_group = ProcessGroup(
                n_threads=self.n_threads,
                complete_only_on_thread_exit=self.complete_only_on_thread_exit,
                slot_wait_interval_seconds=self.slot_wait_interval / 1000,
            )
            self._progress_groups.append(new_group)
            return new_group
        return None

    def submit_message(self, message: Message) -> bool:
        """Submit a single message"""
        submitted = False
        if group := self._acquire_group():  # we have a slot → run it
            submitted = group.submit(
                task=self._task_map[message.task_name],
                message=message,
            )
        return submitted
