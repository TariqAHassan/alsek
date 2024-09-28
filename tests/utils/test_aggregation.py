"""

    Test Aggregation

"""

from typing import Optional

import pytest

from alsek.utils.aggregation import gather_init_params


class Sample:
    def __init__(self, a: int, b: int) -> None:
        self.a = a
        self.b = b


@pytest.mark.parametrize(
    "a,b,ignore",
    [
        (1, 1, None),
        (2, 2, None),
        (3, 4, None),
        (3, 4, "a"),
        (3, 4, "b"),
    ],
)
def test_gather_init_params(a: int, b: int, ignore: Optional[str]) -> None:
    true_params = dict(a=a, b=b)
    sample = Sample(**true_params)
    if ignore:
        true_params.pop(ignore)
    assert gather_init_params(sample, ignore=(ignore,)) == true_params
