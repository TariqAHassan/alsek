"""

    Helpers

"""
from __future__ import annotations

from collections import defaultdict
from typing import Collection, Optional, DefaultDict

from alsek import Broker
from alsek.core.task import Task
from alsek.exceptions import NoTasksFoundError, MultipleBrokersError
from alsek.types import SupportedMechanismType
from alsek.utils.checks import has_duplicates
from alsek.utils.sorting import dict_sort


def filter_tasks(
    tasks: Collection[Task],
    mechanism: SupportedMechanismType,
) -> Collection[Task] | None:
    if not tasks:
        raise NoTasksFoundError("No tasks found")
    elif tasks := [t for t in tasks if t.mechanism == mechanism]:
        return tasks
    else:
        raise NoTasksFoundError(f"No tasks found with mechanism '{mechanism}'.")


def extract_broker(tasks: Collection[Task]) -> Broker:
    if not tasks:
        raise NoTasksFoundError("No tasks found")

    brokers = {t.broker for t in tasks}
    if len(brokers) > 1:
        raise MultipleBrokersError("Multiple brokers used")
    else:
        (broker,) = brokers
        return broker


def derive_consumer_subset(
    tasks: Collection[Task],
    queues: Optional[list[str]],
    task_specific_mode: bool,
) -> dict[str, list[str]] | list[str]:
    if queues and has_duplicates(queues):
        raise ValueError(f"Duplicates in provided queues: {queues}")
    elif queues and not task_specific_mode:
        return queues

    subset: DefaultDict[str, list[str]] = defaultdict(list)
    for t in tasks:
        if queues is None or t.queue in queues:
            subset[t.queue].append(t.name)

    if task_specific_mode:
        return dict_sort(subset, key=queues.index if queues else None)
    else:
        return sorted(subset.keys())
