"""

    Thread Worker Pool

"""

from __future__ import annotations

import dill, logging
from typing import List, Optional, Any

import queue
from alsek import Message
from alsek.core.futures import ThreadTaskFuture, Queue, Event, Process
from alsek.core.pools._base import BaseWorkerPool
from alsek.core.task import Task
from alsek.exceptions import TerminationError
from alsek.utils.system import smart_cpu_count
from builtins import TimeoutError

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
        slot_wait_interval: float = 0.02,
    ) -> None:
        self.q = q
        self.shutdown_event = shutdown_event
        self.n_threads = n_threads
        self.slot_wait_interval = slot_wait_interval

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
            f.stop(TerminationError)
            f.clean_up(ignore_errors=True)

    def run(self) -> None:
        while not self.shutdown_event.is_set():
            # 1. reap finished / timed-out futures
            self._prune()

            # 2. Throttle if thread slots are full
            if not self._has_capacity():
                # Wait *either* for a slot OR the shutdown flag
                self.shutdown_event.wait(self.slot_wait_interval)
                continue

            # 3. Try to pull one unit of work
            try:
                payload = self.q.get(timeout=self.slot_wait_interval)
            except queue.Empty:
                continue

            # 4. Launch a new ThreadTaskFuture
            self._spawn_future(payload)

        self._stop_all_live_futures()


def _start_thread_worker(
    q: Queue,
    shutdown_event: Event,
    n_threads: int,
    slot_wait_interval: float,
) -> None:
    # We construct the worker *inside* the child so we don’t have to pickle it.
    worker = ThreadInProcessGroup(
        q=q,
        shutdown_event=shutdown_event,
        n_threads=n_threads,
        slot_wait_interval=slot_wait_interval,
    )
    worker.run()


class ProcessGroup:
    def __init__(
        self,
        n_threads: int,
        complete_only_on_thread_exit: bool,
        slot_wait_interval: float = 0.02,
    ) -> None:
        self._n_threads = n_threads
        self.complete_only_on_thread_exit = complete_only_on_thread_exit
        self.slot_wait_interval = slot_wait_interval

        self.queue: Queue = Queue(maxsize=n_threads)
        self.shutdown_event: Event = Event()
        self.process = Process(
            target=_start_thread_worker,
            args=(
                self.queue,
                self.shutdown_event,
                n_threads,
                slot_wait_interval,
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

    def stop(self, timeout: int = 2) -> None:
        # 1. Signal
        self.shutdown_event.set()
        # 2. Wait a bit for graceful exit
        self.process.join(timeout=timeout)
        # 3. Hard kill if still alive
        if self.process.is_alive():
            self.process.kill()


class ThreadWorkerPool(BaseWorkerPool):
    """Elastic thread-based pool.

    • Spawns a new **process** (ThreadProcessGroup) only when all existing
      groups are saturated and the hard ceiling `n_processes` hasn’t been hit.

    • Each group runs up to `n_threads` true ThreadTaskFutures concurrently.
    """

    def __init__(
        self,
        *,
        n_threads: int = 8,
        n_processes: Optional[int] = None,
        complete_only_on_thread_exit: bool = False,
        slot_wait_interval: float = 0.05,
        **kwargs: Any,
    ) -> None:
        super().__init__(mechanism="thread", **kwargs)
        self.n_threads = n_threads
        self.n_processes = n_processes or smart_cpu_count()
        self.slot_wait_interval = slot_wait_interval
        self.complete_only_on_thread_exit = complete_only_on_thread_exit

        self._groups: List[ProcessGroup] = list()

    def stop_all_futures(self) -> None:
        for g in self._groups:
            g.stop()

    def _prune(self) -> None:
        updated_groups = list()
        for g in self._groups:
            g.process.join(timeout=0)  # reap quickly if already exited
            if g.process.is_alive():
                updated_groups.append(g)
        self._groups = updated_groups

    def _acquire_group(self) -> Optional[ProcessGroup]:
        for g in self._groups:
            if g.has_slot():
                return g

        if len(self._groups) < self.n_processes:
            new_group = ProcessGroup(
                n_threads=self.n_threads,
                complete_only_on_thread_exit=self.complete_only_on_thread_exit,
                slot_wait_interval=self.slot_wait_interval,
            )
            self._groups.append(new_group)
            return new_group
        return None

    def engine(self) -> None:
        while not self.stop_signal.received:
            for message in self.stream():
                self._prune()

                if group := self._acquire_group():  # we have a slot → run it
                    successfully_submitted = group.submit(
                        task=self._task_map[message.task_name],
                        message=message,
                    )
                    if successfully_submitted:
                        continue

                # Saturated: free message & retry later
                if message.lock_long_name:
                    message.unlink_lock(
                        missing_ok=True,
                        target_backend=self.broker.backend,
                    )
                self.stop_signal.wait(self.slot_wait_interval)  # back-off once
                # Break so we start the stream again from the beginning.
                # This is important because the stream is ordered by priority.
                # That is, when we finally can acquire a group again, we want
                # to saturate it by message priority (high -> low).
                break
