"""

    Command Line Interface

"""

import logging
from typing import Optional

import click

from alsek import __version__
from alsek.core.pools.process import ProcessWorkerPool
from alsek.core.pools.thread import ThreadWorkerPool
from alsek.utils.logging import setup_logging
from alsek.utils.scanning import collect_tasks

LOG_LEVELS = list(logging._nameToLevel.keys())  # noqa


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Alsek worker pool CLI."""
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
    log_level: str,
) -> None:
    """Start a process-based worker pool."""
    setup_logging(logging.getLevelName(log_level.upper().strip()))

    pool = ProcessWorkerPool(
        tasks=collect_tasks(package),
        queues=[q.strip() for q in queues.split(",")] if queues else None,
        task_specific_mode=task_specific_mode,
        n_processes=n_processes,
        prune_interval=prune_interval,
        slot_wait_interval=slot_wait_interval,
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
    slot_wait_interval: int,
    complete_only_on_thread_exit: bool,
    log_level: str,
) -> None:
    """Start a thread-based worker pool."""
    setup_logging(logging.getLevelName(log_level.upper().strip()))

    pool = ThreadWorkerPool(
        tasks=collect_tasks(package),
        queues=[q.strip() for q in queues.split(",")] if queues else None,
        task_specific_mode=task_specific_mode,
        n_threads=n_threads,
        n_processes=n_processes,
        slot_wait_interval=slot_wait_interval,
        complete_only_on_thread_exit=complete_only_on_thread_exit,
    )
    pool.run()


if __name__ == "__main__":
    main()
