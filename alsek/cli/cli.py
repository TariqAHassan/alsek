"""

    Command Line Interface

"""

from typing import Optional

import click

from alsek import __version__
from alsek.core.backoff import LinearBackoff
from alsek.core.worker import WorkerPool
from alsek.utils.logging import setup_logging
from alsek.utils.scanning import collect_tasks, parse_logging_level


@click.command()
@click.version_option(__version__)
@click.argument("module", type=str)
@click.option(
    "-q",
    "--queues",
    type=str,
    default=None,
    help="Comma separated list of queues to consume from. "
    "If null, all queues will be consumed.",
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
    help="amount of time (in milliseconds) to wait between checks to "
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
@click.option("-q", "--quiet", is_flag=True, help="Disable detailed logging.")
def main(
    module: str,
    queues: Optional[str],  # noqa
    max_threads: int,  # noqa
    max_processes: Optional[int],  # noqa
    management_interval: int,  # noqa
    slot_wait_interval: int,  # noqa
    consumer_backoff_factor: int,  # noqa
    consumer_backoff_floor: int,  # noqa
    consumer_backoff_ceiling: int,  # noqa
    debug: bool,  # noqa
    quiet: bool,  # noqa
) -> None:
    """Start a pool of Alsek workers.

    Arguments:

        module: module(s) to scan for task definitions

    Examples:

        * alsek my_package.tasks

        * alsek my_package.tasks --processes 4

        * alsek my_package.tasks --q queue_a,queue_b

    """
    setup_logging(parse_logging_level(debug, verbose=not quiet))

    WorkerPool(
        tasks=collect_tasks(module),
        queues=[i.strip() for i in queues.split(",")] if queues else None,
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


if __name__ == "__main__":
    main()
