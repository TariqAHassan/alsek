"""

    Integrity

"""
from __future__ import annotations

from typing import Union

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from alsek import StatusTracker, Broker
from alsek.core.message import Message
from alsek.core.status.types import TaskStatus, TERMINAL_TASK_STATUSES


def _name2message(name: str) -> Message:
    *_, queue, task_name, uuid = name.split(":")
    return Message(task_name, queue=queue, uuid=uuid)


class StatusTrackerIntegrityScanner:
    """Tool to ensure the integrity of statuses scanning a ``StatusTracker()``
    with non-terminal statuses (i.e., ``TaskStatus.FAILED`` or ``TaskStatus.SUCCEEDED``)
    that no longer exist in the broker. Entries which meet this criteria will have
    their status set to ``TaskStatus.UNKNOWN``.

    Args:
        status_tracker (StatusTracker): status tracker to scan for messages with non-terminal status
        trigger (CronTrigger, DateTrigger, IntervalTrigger, optional):
            trigger which determines how often to perform the scan.

    """

    def __init__(
        self,
        status_tracker: StatusTracker,
        broker: Broker,
        trigger: Union[CronTrigger, DateTrigger, IntervalTrigger] = IntervalTrigger(hours=1),  # fmt: skip
    ) -> None:
        self.status_tracker = status_tracker
        self.broker = broker
        self.trigger = trigger

        if status_tracker.backend.IS_ASYNC:
            raise ValueError("Async backends not supported")

        self.scheduler: BackgroundScheduler = BackgroundScheduler()
        if trigger:
            self.scheduler.start()
            self.scheduler.add_job(
                self.scan,
                trigger=trigger,
                id="integrity_scan",
            )

    def scan(self) -> None:
        """Run the integrity scan.

        Returns:
            None

        """
        for name in self.status_tracker.backend.scan("status*"):
            message = _name2message(name)
            status = self.status_tracker.get(message).status
            if (
                status is not None
                and status not in TERMINAL_TASK_STATUSES
                and not self.broker.exists(message)
            ):
                self.status_tracker.set(message, status=TaskStatus.UNKNOWN)
