"""

    Command Line Interface

"""

import logging
from typing import Optional

import click

from alsek import __version__
from alsek.core.backoff import LinearBackoff
from alsek.core.worker.process import ProcessWorkerPool
from alsek.core.worker.thread import ThreadWorkerPool
from alsek.utils.logging import setup_logging
from alsek.utils.scanning import collect_tasks

LOG_LEVELS = list(logging._nameToLevel.keys())  # noqa


def _apply_logging_level(log_level: str) -> None:
    log_level_value: int = logging.getLevelName(log_level.upper().strip())
    setup_logging(log_level_value)


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Alsek CLI."""
    pass


@main.command()
@click.argument("package", type=str)
@click.option(
    "--queues",
    type=str,
    default=None,
    help="Comma-separated list of queues to consume from.",
)
@click.option(
    "--task_specific_mode",
    is_flag=True,
    help="Monitor tasks specifically, not just queues.",
)
@click.option(
    "--n_processes",
    type=int,
    default=None,
    help="Max number of processes.",
)
@click.option(
    "--prune_interval",
    type=int,
    default=100,
    help="Milliseconds between prune scans.",
)
@click.option(
    "--slot_wait_interval",
    type=int,
    default=100,
    help="Milliseconds to wait when full.",
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
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default="INFO",
    help="Logging level.",
)
def process_pool(
    package: str,
    queues: Optional[str],
    task_specific_mode: bool,
    n_processes: Optional[int],
    prune_interval: int,
    slot_wait_interval: int,
    consumer_backoff_factor: int,
    consumer_backoff_floor: int,
    consumer_backoff_ceiling: int,
    log_level: str,
) -> None:
    """Start a process-based worker pool."""
    _apply_logging_level(log_level)

    pool = ProcessWorkerPool(
        tasks=collect_tasks(package),
        queues=[q.strip() for q in queues.split(",")] if queues else None,
        task_specific_mode=task_specific_mode,
        n_processes=n_processes,
        prune_interval=prune_interval,
        slot_wait_interval=slot_wait_interval,
        backoff=LinearBackoff(
            factor=consumer_backoff_factor,
            floor=consumer_backoff_floor,
            ceiling=consumer_backoff_ceiling,
            zero_override=False,
        ),
    )
    pool.run()


@main.command()
@click.argument("package", type=str)
@click.option(
    "--queues",
    type=str,
    default=None,
    help="Comma-separated list of queues to consume from.",
)
@click.option(
    "--task_specific_mode",
    is_flag=True,
    help="Monitor tasks specifically, not just queues.",
)
@click.option(
    "--n_threads",
    type=int,
    default=8,
    help="Threads per group.",
)
@click.option(
    "--n_processes",
    type=int,
    default=None,
    help="Max process groups.",
)
@click.option(
    "--n_process_floor",
    type=int,
    default=1,
    help="Minimum number of process groups to keep alive.",
)
@click.option(
    "--slot_wait_interval",
    type=int,
    default=50,
    help="Milliseconds to wait when full.",
)
@click.option(
    "--complete_only_on_thread_exit",
    is_flag=True,
    help="Wait for thread exit to mark as complete.",
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
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default="INFO",
    help="Logging level.",
)
def thread_pool(
    package: str,
    queues: Optional[str],
    task_specific_mode: bool,
    n_threads: int,
    n_processes: Optional[int],
    n_process_floor: int,
    slot_wait_interval: int,
    complete_only_on_thread_exit: bool,
    consumer_backoff_factor: int,
    consumer_backoff_floor: int,
    consumer_backoff_ceiling: int,
    log_level: str,
) -> None:
    """Start a thread-based worker pool."""
    _apply_logging_level(log_level)

    pool = ThreadWorkerPool(
        tasks=collect_tasks(package),
        queues=[q.strip() for q in queues.split(",")] if queues else None,
        task_specific_mode=task_specific_mode,
        n_threads=n_threads,
        n_processes=n_processes,
        n_process_floor=n_process_floor,
        slot_wait_interval=slot_wait_interval,
        complete_only_on_thread_exit=complete_only_on_thread_exit,
        package_name=package,
        backoff=LinearBackoff(
            factor=consumer_backoff_factor,
            floor=consumer_backoff_floor,
            ceiling=consumer_backoff_ceiling,
            zero_override=False,
        ),
    )
    pool.run()


if __name__ == "__main__":
    main()
