"""

    Command Line Interface

"""

import ast
import os
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import Callable, Optional

import click
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from alsek import __version__
from alsek.core.backoff import LinearBackoff
from alsek.core.worker import WorkerPool
from alsek.utils.logging import setup_logging
from alsek.utils.scanning import collect_tasks, parse_logging_level

WATCHED_FILE_EXTENSIONS = (".py",)


class RestartOnChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_callback: Callable[[], None]) -> None:
        self.restart_callback = restart_callback

    @staticmethod
    def _has_valid_syntax(file_path: str) -> bool:
        try:
            source = Path(file_path).open("r", encoding="utf-8").read()
            ast.parse(source, filename=file_path)
            return True
        except SyntaxError:
            return False

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory and event.src_path.endswith(WATCHED_FILE_EXTENSIONS):
            if self._has_valid_syntax(event.src_path):
                click.echo(f"Detected changes in {event.src_path}. Restarting worker pool...")  # fmt: skip
                self.restart_callback()
            else:
                click.echo(f"Skipping restart due to syntax errors in {event.src_path}...")  # fmt: skip


def _package2path(package: str) -> Path:
    """Convert a Python package name into its corresponding filesystem path."""
    spec = find_spec(package)
    if spec is None or spec.origin is None:
        raise ModuleNotFoundError(f"Package '{package}' not found.")

    path = Path(spec.origin)
    return path.parent if path.name == "__init__.py" else path


def _configure_reload(package: str) -> Observer:
    sys.path.append(os.getcwd())
    directory = _package2path(package)
    if not directory.is_dir():
        raise NotADirectoryError(f"The provided path '{str(directory)}' is not a directory")  # fmt: skip

    def restart_program() -> None:
        os.execv(sys.executable, [sys.executable] + sys.argv)

    observer = Observer()
    handler = RestartOnChangeHandler(restart_callback=restart_program)
    observer.schedule(handler, path=str(directory), recursive=True)
    observer.start()
    return observer


@click.command()
@click.version_option(__version__)
@click.argument("package", type=str)
@click.option(
    "-qu",
    "--queues",
    type=str,
    default=None,
    help="Comma separated list of queues to consume from. "
    "If null, all queues will be consumed.",
)
@click.option(
    "-tsm",
    "--task_specific_mode",
    is_flag=True,
    help="Narrowly monitor the tasks (true) or queues more broadly (false; default)",
)
@click.option(
    "--max_threads",
    type=int,
    default=8,
    help="Maximum of task with mechanism='thread' supported at any 'one' time.",
)
@click.option(
    "--max_processes",
    type=int,
    default=None,
    help="Maximum of task with mechanism='process' supported at"
    "any one time. Defaults to max(1, CPU_COUNT - 1).",
)
@click.option(
    "--management_interval",
    type=int,
    default=100,
    help="Amount of time (in milliseconds) between maintenance "
    "scans of background task execution.",
)
@click.option(
    "--slot_wait_interval",
    type=int,
    default=100,
    help="Amount of time (in milliseconds) to wait between checks to "
    "determine if a process for task execution is available.",
)
@click.option(
    "--consumer_backoff_factor",
    type=int,
    default=1 * 1000,
    help="Backoff factor in response to passes over the backend "
    "which yield no messages (milliseconds)",
)
@click.option(
    "--consumer_backoff_floor",
    type=int,
    default=1_000,
    help="Minimum backoff in response to a pass over the backend"
    "which yields no message (milliseconds)",
)
@click.option(
    "--consumer_backoff_ceiling",
    type=int,
    default=30_000,
    help="Maximum backoff in response to a pass over the backend"
    "which yields no message (milliseconds)",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debugging logging",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    help="Disable detailed logging.",
)
@click.option(
    "-r",
    "--reload",
    is_flag=True,
    help="Watch for changes and automatically reload if changes are detected.",
)
def main(
    package: str,
    queues: Optional[str],  # noqa
    task_specific_mode: bool,  # noqa
    max_threads: int,  # noqa
    max_processes: Optional[int],  # noqa
    management_interval: int,  # noqa
    slot_wait_interval: int,  # noqa
    consumer_backoff_factor: int,  # noqa
    consumer_backoff_floor: int,  # noqa
    consumer_backoff_ceiling: int,  # noqa
    debug: bool,  # noqa
    quiet: bool,  # noqa
    reload: bool,  # noqa
) -> None:
    """Start a pool of Alsek workers.

    Arguments:

        package: the package to scan for task definitions

    Examples:

        * alsek my_package.tasks

        * alsek my_package.tasks --processes 4

        * alsek my_package.tasks --qu queue_a,queue_b

    """
    setup_logging(parse_logging_level(debug, verbose=not quiet))

    observer = None
    if reload:
        try:
            observer = _configure_reload(package)
        except NotADirectoryError as error:
            click.echo(f"Error: {str(error)}")
            return

    try:
        WorkerPool(
            tasks=collect_tasks(package),
            queues=[i.strip() for i in queues.split(",")] if queues else None,
            task_specific_mode=task_specific_mode,
            max_threads=max_threads,
            max_processes=max_processes,
            management_interval=management_interval,
            slot_wait_interval=slot_wait_interval,
            backoff=LinearBackoff(
                factor=consumer_backoff_factor,
                floor=consumer_backoff_floor,
                ceiling=consumer_backoff_ceiling,
                zero_override=False,
            ),
        ).run()
    finally:
        if observer:
            observer.stop()
            observer.join()


if __name__ == "__main__":
    main()
