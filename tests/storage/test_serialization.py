"""

    Test Serialization

"""
from typing import Any, List, Tuple

import pytest

from alsek.storage.serialization import JsonSerializer, Serializer
from tests._helpers import expand_params_factory

_ALL_SERIALIZERS = (JsonSerializer(),)
_expand_to_all_serializers = expand_params_factory(_ALL_SERIALIZERS)


@pytest.mark.parametrize(
    "value,seralizer",
    _expand_to_all_serializers(
        1,
        1.0,
        "string",
        [[1, 2, 3]],
        [[{"a": 1}, {"b": 2}]],
        [["list", "of", "strings"]],
        {"a": 1},
        {"a": [1, 2, 3]},
    ),
)
def test_seralizer(value: Any, seralizer: Serializer) -> None:
    assert seralizer.reverse(seralizer.forward(value)) == value
