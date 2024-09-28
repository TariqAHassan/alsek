"""

    Test Logging

"""

import logging
from typing import Any, Callable

import pytest

from alsek.utils.logging import (
    _merge_args_kwargs,
    _run_func,
    get_logger,
    magic_logger,
    setup_logging,
)


def test_get_logger() -> None:
    assert isinstance(get_logger(), logging.Logger)


@pytest.mark.parametrize(
    "level",
    [
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ],
)
def test_setup_logging(level: int) -> None:
    setup_logging(level)
    assert get_logger().level == level


@pytest.mark.parametrize(
    "function,args,kwargs,expected",
    [
        (lambda a: None, [1], {}, {"a": 1}),
        (lambda a: None, [], {"a": 1}, {"a": 1}),
        (lambda a, b: None, [1, 1], {}, {"a": 1, "b": 1}),
        (lambda a, b: None, [], {"a": 1, "b": 1}, {"a": 1, "b": 1}),
    ],
)
def test_merge_args_kwargs(
    function: Callable[..., Any],
    args: list[Any],
    kwargs: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    actual = _merge_args_kwargs(function, args=args, kwargs=kwargs)
    assert actual == expected


@pytest.mark.parametrize(
    "function,kwargs,expected",
    [
        (lambda a: a, {"a": 1}, 1),
        (lambda a, b: a + b, {"a": 1, "b": 1}, 2),
    ],
)
def test_run_func(
    function: Callable[..., Any],
    kwargs: dict[str, Any],
    expected: Any,
) -> None:
    actual = _run_func(function, **kwargs)
    assert actual == expected


def test_magic_logger() -> None:
    a, b = list(), list()

    def before_func() -> None:
        a.append(1)

    def after_func() -> None:
        b.append(1)

    @magic_logger(before=before_func, after=after_func)
    def func():
        pass

    func()
    assert a == b == [1]
