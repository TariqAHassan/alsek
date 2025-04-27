"""

    Test String

"""

import pytest

from alsek.utils.string import smart_join


@pytest.mark.parametrize(
    "items, limit, delimiter, expected",
    [
        (
            ["apple"],
            5,
            ", ",
            "apple",
        ),
        (
            ["apple", "banana"],
            5,
            ", ",
            "apple and banana",
        ),
        (
            ["a", "b", "c", "d", "e"],
            5,
            ", ",
            "a, b, c, d and e",
        ),
        (
            ["a", "b", "c", "d", "e", "f"],
            5,
            ", ",
            "a, b, c, d, e...",
        ),
        (
            ["a", "b", "c", "d", "e", "f"],
            3,
            " | ",
            "a | b | c...",
        ),
        # shows why custom delimiters + "and" are tricky
        (
            ["one", "two"],
            5,
            " + ",
            "one and two",
        ),
    ],
)
def test_smart_join(
    items: list[str],
    limit: int,
    delimiter: str,
    expected: str,
) -> None:
    actual = smart_join(items, limit=limit, delimiter=delimiter)
    assert actual == expected
