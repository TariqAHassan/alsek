"""

    Test Serialization

"""

from typing import Any, Optional

import pytest

from alsek.storage.serialization import BinarySerializer, JsonSerializer, Serializer
from tests._helpers import expand_params_factory

_ALL_SERIALIZERS = (
    JsonSerializer(),
    BinarySerializer(),
)
_expand_to_all_serializers = expand_params_factory(_ALL_SERIALIZERS)


@pytest.mark.parametrize(
    "value,serializer",
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
@pytest.mark.parametrize("compression_level", [None, *range(10)])
def test_serializer(
    value: Any, serializer: Serializer, compression_level: Optional[int]
) -> None:
    serializer.compression_level = compression_level

    fwd = serializer.forward(value)
    rev = serializer.reverse(fwd)
    assert rev == value
