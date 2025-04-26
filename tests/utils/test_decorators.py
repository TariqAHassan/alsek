"""

    Test Decorators

"""

import pytest

from alsek.utils.decorators import exception_suppressor


@exception_suppressor(KeyError)
def get_key(d: dict[str, int], key: str) -> int | None:
    return d[key]


@exception_suppressor(ZeroDivisionError, ValueError)
def safe_div(num: int, denom: int) -> float | None:
    return num / denom


@exception_suppressor(ValueError)
def boom() -> None:
    raise KeyError("not suppressed")  # different error → should propagate


def test_suppressed_exception_returns_none() -> None:
    assert get_key({}, "missing") is None
    assert safe_div(1, 0) is None


def test_success_passes_value() -> None:
    assert get_key({"x": 5}, "x") == 5
    assert safe_div(10, 2) == 5.0


def test_unsuppressed_exception_propagates() -> None:
    with pytest.raises(KeyError):
        boom()


def test_wraps_preserves_metadata() -> None:
    assert get_key.__name__ == "get_key"
    assert safe_div.__doc__ is None  # original docstring (None) preserved
