"""

    Test String Utils

"""
from typing import Optional

import pytest

from alsek.utils.string import name_matcher


@pytest.mark.parametrize(
    "pattern,name",
    [
        (None, "cats"),
        ("cats", "cats"),
        ("*cats", "happy_cats"),
        ("cats*", "cats_happy"),
        ("*cats*", "many cats happy"),
    ],
)
def test_name_matcher(pattern: Optional[str], name: str) -> None:
    assert name_matcher(pattern, name=name)
