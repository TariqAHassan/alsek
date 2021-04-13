"""

    Helpers

"""
import logging
from importlib import import_module
from inspect import getmembers
from pkgutil import walk_packages
from types import ModuleType
from typing import Any, Dict, Iterable, Tuple

from alsek._utils.logging import magic_logger
from alsek.core.task import Task
from alsek.exceptions import NoTasksFoundError, TaskNameCollisionError

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
    before=lambda name: log.debug("Scanning %r for tasks...", name),
    after=lambda output: log.debug(
        "Found %s task%s.",
        len(output),
        "s" if len(output) > 1 else "",
    ),
)
def collect_tasks(name: str) -> Tuple[Task, ...]:
    """Recursively collect all tasks in ``name``.

    Args:
        name (str): name of a module

    Returns:
        tasks (Tuple[Task, ...]): collected tasks

    Raises:
        NoTasksFoundError: if no tasks can be found

    """
    all_tasks: Dict[str, Task] = dict()
    for module in _enumerate_modules(import_module(name)):
        for name, task in getmembers(module, _is_task):
            if name in all_tasks:
                if task != all_tasks[name]:
                    raise TaskNameCollisionError(f"Multiple tasks '{name}'")
            else:
                all_tasks[name] = task

    if all_tasks:
        return tuple(all_tasks.values())
    else:
        raise NoTasksFoundError("No tasks found")
