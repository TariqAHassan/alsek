"""

    Test Backoff

"""

from inspect import signature
from typing import Type

import pytest
from schema import Schema

from alsek.core.backoff import (
    Backoff,
    ConstantBackoff,
    ExponentialBackoff,
    LinearBackoff,
    _get_algorithm,
    settings2backoff,
)
from tests._helpers import expand_params_factory

ALL_BACKOFF_ALGORITHMS = tuple(Backoff.__subclasses__())

backoff_expander = expand_params_factory(ALL_BACKOFF_ALGORITHMS)


def _extract_algorithm_specific_params(backoff: Type[Backoff]) -> tuple[str, ...]:
    all_parameters = signature(backoff.__init__).parameters
    return tuple(p for p in all_parameters if p not in ("self", "kwargs"))


@pytest.mark.parametrize(
    "zero_override,backoff",
    backoff_expander(True, False),
)
def test_floor_and_ceiling(
    zero_override: bool,
    backoff: Type[Backoff],
) -> None:
    actual = backoff(floor=1, ceiling=1, zero_override=zero_override).get(0)
    if zero_override:
        assert actual == 0
    else:
        assert actual == 1


@pytest.mark.parametrize(
    "amount,floor,ceiling,backoff",
    backoff_expander(
        # Test amount < floor
        (10, 100, 500),
        # Test amount > ceiling
        (10, 0, 5),
        # Test floor == ceiling
        (100, 0, 0),
    ),
)
def test_clipper(
    amount: int,
    floor: int,
    ceiling: int,
    backoff: Type[Backoff],
) -> None:
    algorithm = backoff(floor=floor, ceiling=ceiling, zero_override=False)
    if amount < floor:
        assert algorithm._clipper(amount) == floor
    elif amount > ceiling:
        assert algorithm._clipper(amount) == ceiling
    else:
        assert floor <= algorithm._clipper(amount) <= ceiling


@pytest.mark.parametrize(
    "backoff",
    ALL_BACKOFF_ALGORITHMS,
)
def test_parameters(backoff: Type[Backoff]) -> None:
    expected_keys = {
        "floor",
        "ceiling",
        "zero_override",
        *_extract_algorithm_specific_params(backoff),
    }
    assert set(backoff().parameters) == expected_keys


@pytest.mark.parametrize(
    "backoff",
    ALL_BACKOFF_ALGORITHMS,
)
def test_settings(backoff: Type[Backoff]) -> None:
    algorithm = backoff()
    Schema(
        {
            "algorithm": algorithm.__class__.__name__,
            "parameters": algorithm.parameters,
        }
    ).validate(algorithm.settings)


@pytest.mark.parametrize(
    "incidents,expected,backoff",
    [
        # Test constant
        (0, 100, ConstantBackoff(constant=100)),
        (1, 100, ConstantBackoff(constant=100)),
        (2, 100, ConstantBackoff(constant=100)),
        # Test Linear
        (1, 1, LinearBackoff(factor=1)),
        (1, 2, LinearBackoff(factor=2)),
        (1, 3, LinearBackoff(factor=3)),
        # Text expoential
        (0, 1, ExponentialBackoff(factor=1, base=1)),
        (1, 4, ExponentialBackoff(factor=2, base=2)),
        (3, 27, ExponentialBackoff(factor=1, base=3)),
    ],
)
def test_formula(incidents: int, expected: int, backoff: Backoff) -> None:
    assert backoff.formula(incidents) == expected


@pytest.mark.parametrize("name", [i.__name__ for i in ALL_BACKOFF_ALGORITHMS])
def test_get_algorithm(name: str) -> None:
    assert _get_algorithm(name).__name__ == name


@pytest.mark.parametrize("backoff", ALL_BACKOFF_ALGORITHMS)
def test_get_algorithm(backoff: Type[Backoff]) -> None:
    # Test for a veridical reconstruction of the settings
    settings = backoff().settings
    assert settings2backoff(settings).settings == settings
