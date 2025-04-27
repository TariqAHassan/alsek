"""

    Helpers

"""

from __future__ import annotations

import logging
import os
import sys
from importlib import import_module
from inspect import getmembers
from pkgutil import walk_packages
from types import ModuleType
from typing import Any, Iterable

from alsek.core.task import Task
from alsek.exceptions import NoTasksFoundError, TaskNameCollisionError
from alsek.utils.logging import magic_logger

log = logging.getLogger(__name__)


def _enumerate_modules(module: ModuleType) -> Iterable[ModuleType]:
    module_path = getattr(module, "__path__", None)
    if module_path is None:
        yield module
    else:
        for _, name, is_pkg in walk_packages(module_path, f"{module.__name__}."):
            if not is_pkg:
                yield import_module(name)


def _is_task(obj: Any) -> bool:
    return isinstance(obj, Task)


@magic_logger(
    before=lambda module: log.debug("Scanning %r for tasks...", module),
    after=lambda output: log.debug(
        "Found %s task%s.",
        len(output),
        "s" if len(output) > 1 else "",
    ),
)
def collect_tasks(module: str | ModuleType) -> tuple[Task, ...]:
    """Recursively collect all tasks in ``name``.

    Args:
        module (str, ModuleType): name of a module

    Returns:
        module (tuple[Task, ...]): collected tasks

    Raises:
        NoTasksFoundError: if no tasks can be found

    """
    sys.path.append(os.getcwd())
    if isinstance(module, str):
        module = import_module(module)
    elif not isinstance(module, ModuleType):
        raise TypeError(f"Unsupported input type, got {type(module)}")

    all_tasks: dict[str, Task] = dict()
    for m in _enumerate_modules(module):
        for name, task in getmembers(m, predicate=_is_task):
            if name in all_tasks:
                if task != all_tasks[name]:
                    raise TaskNameCollisionError(f"Multiple tasks '{name}'")
            else:
                all_tasks[name] = task

    if all_tasks:
        return tuple(v for _, v in all_tasks.items())
    else:
        raise NoTasksFoundError("No tasks found")
