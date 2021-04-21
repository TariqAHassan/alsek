"""

    Test CLI Helpers

"""
import logging
from typing import Set

import pytest

from alsek.cli._helpers import collect_tasks, parse_logging_level


@pytest.mark.parametrize(
    "debug,verbose,expected",
    [
        # Debug
        (True, False, logging.DEBUG),
        (True, True, logging.DEBUG),
        # Info (verbose)
        (False, True, logging.INFO),
        # All other cases
        (False, False, logging.ERROR),
    ],
)
def test_parse_logging_level(
    debug: bool,
    verbose: bool,
    expected: int,
) -> None:
    assert parse_logging_level(debug, verbose=verbose) is expected


@pytest.mark.parametrize(
    "name,expected",
    [
        ("examples.simple", {"add"}),
        ("examples.ml", {"predict"}),
    ],
)
def test_collect_tasks(name: str, expected: Set[str]) -> None:
    actual = collect_tasks(name)
    assert {i.name for i in actual} == expected
