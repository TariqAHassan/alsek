"""

    Test CLI Helpers

"""

import logging
from typing import Set

import pytest

from alsek.utils.scanning import collect_tasks


@pytest.mark.parametrize(
    "module,expected",
    [
        ("examples.simple", {"add"}),
        ("examples.ml", {"predict"}),
    ],
)
def test_collect_tasks(module: str, expected: Set[str]) -> None:
    actual = collect_tasks(module)
    assert {i.name for i in actual} == expected
