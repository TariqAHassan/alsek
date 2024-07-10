"""

    Test Checks

"""
from typing import Any, Collection

import pytest

from alsek.utils.checks import has_duplicates


@pytest.mark.parametrize(
    "itera,expected",
    [
        ([1, 2, 3], False),
        ({1, 2, 3}, False),
        ([1, 2, 3, 3], True),
        (["a", "b", "c"], False),
        (["a", "b", "c", "c"], True),
    ],
)
def test_has_duplicates(
    itera: Collection[Any],
    expected: bool,
) -> bool:
    assert has_duplicates(itera) is expected
