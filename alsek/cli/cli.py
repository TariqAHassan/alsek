"""

    Command Line Interface

"""

from typing import Optional, Callable
import click
import sys
import os
from pathlib import Path
from alsek import __version__
from watchdog.observers import Observer
from alsek.core.backoff import LinearBackoff
from alsek.core.worker import WorkerPool
from alsek.utils.logging import setup_logging
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from alsek.utils.scanning import collect_tasks, parse_logging_level


class RestartOnChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_callback: Callable[[], None]) -> None:
        self.restart_callback = restart_callback

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            click.echo(f"Detected changes in {event.src_path}. Restarting worker pool...")  # fmt: skip
            self.restart_callback()


def _configure_watch(watch: str) -> Observer:
    """Configure and start a watchdog observer."""
    if not Path(watch).is_dir():
        raise NotADirectoryError(f"The provided path '{watch}' is not a directory")

    def restart_program() -> None:
        os.execv(sys.executable, [sys.executable] + sys.argv)

    observer = Observer()
    handler = RestartOnChangeHandler(restart_callback=restart_program)
    observer.schedule(handler, path=watch, recursive=True)
    observer.start()
    click.echo(f"Watching directory {watch} for changes...")
    return observer


@click.command()
@click.version_option(__version__)
@click.argument("module", type=str)
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
    "--watch",
    type=str,
    help="Directory to monitor for changes. If changes are detected, the worker pool restarts.",
)
def main(
    module: str,
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
    watch: Optional[str],  # noqa
) -> None:
    """Start a pool of Alsek workers.

    Arguments:

        module: module(s) to scan for task definitions

    Examples:

        * alsek my_package.tasks

        * alsek my_package.tasks --processes 4

        * alsek my_package.tasks --qu queue_a,queue_b

    """
    setup_logging(parse_logging_level(debug, verbose=not quiet))

    # Set up file watching, if a watch directory is provided
    observer = None
    if watch:
        try:
            observer = _configure_watch(watch)
        except NotADirectoryError as error:
            click.echo(f"Error: {str(error)}")
            return

    try:
        WorkerPool(
            tasks=collect_tasks(module),
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
