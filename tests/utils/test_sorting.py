"""

    Test Sorting

"""

from typing import Any, Callable

import pytest

from alsek.utils.sorting import dict_sort


@pytest.mark.parametrize(
    "input_,key,expected",
    [
        ({"a": 1, "b": 2}, lambda k: k, {"a": 1, "b": 2}),
        ({2: "a", 1: "b"}, lambda k: k, {1: "b", 2: "a"}),
        ({2: "a", 1: "b"}, lambda k: -k, {2: "a", 1: "b"}),
    ],
)
def test_dict_sort(
    input_: dict[Any, Any],
    key: Callable[[Any], Any],
    expected: dict[Any, Any],
) -> None:
    assert dict_sort(input_, key=key) == expected
