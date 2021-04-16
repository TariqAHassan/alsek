"""

    Test CLI Helpers

"""
from typing import Set

import pytest

from alsek.cli._helpers import collect_tasks


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
