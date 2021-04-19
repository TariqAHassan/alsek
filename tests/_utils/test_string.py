"""

    Test String Utils
    ~~~~~~~~~~~~~~~~~

"""
import pytest
from alsek._utils.string import name_matcher
from typing import Optional


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
