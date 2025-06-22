"""

    Test Helpers

"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest  # noqa

from alsek.utils.helpers import dict_merge_update_into_origin


@pytest.mark.parametrize(
    ("origin", "update", "inplace", "expected"),
    [
        ({}, {"a": 1}, False, {"a": 1}),
        ({"x": 0}, {"y": 2, "z": 3}, False, {"x": 0, "y": 2, "z": 3}),
        ({}, {}, False, {}),
    ],
)
def test_merge_creates_new_dict(
    origin: dict[str, int],
    update: dict[str, int],
    inplace: bool,
    expected: dict[str, int],
) -> None:
    orig_copy = deepcopy(origin)
    result = dict_merge_update_into_origin(
        origin,
        update=update,
        inplace=inplace,
    )
    # result matches expected
    assert result == expected
    # original unmodified when inplace is False
    assert origin == orig_copy


@pytest.mark.parametrize(
    ("origin", "update"),
    [
        ({"a": [1, 2]}, {"b": [3]}),
        ({"nested": {"x": 1}}, {"new": {"y": 2}}),
    ],
)
def test_deepcopy_behavior(
    origin: dict[str, list[int] | dict[str, int]], update: dict[str, Any]
) -> None:
    orig_copy = deepcopy(origin)
    result = dict_merge_update_into_origin(origin, update, inplace=False)
    # mutating result should not affect original
    if isinstance(result.get(next(iter(update))), list):
        result[next(iter(update))].append(99)
        assert origin == orig_copy
    else:
        result[next(iter(update))]["added"] = True
        assert origin == orig_copy


@pytest.mark.parametrize(
    ("origin", "update"),
    [
        ({"a": 1}, {"b": 2}),
        ({}, {"x": None}),
    ],
)
def test_inplace_mutates_original(
    origin: dict[str, Any],
    update: dict[str, Any],
) -> None:
    orig_id = id(origin)
    result = dict_merge_update_into_origin(origin, update=update, inplace=True)
    # same object returned
    assert id(result) == orig_id
    # origin updated
    assert all(item in origin.items() for item in update.items())


@pytest.mark.parametrize(
    ("origin", "update"),
    [
        ({"a": 1}, {"a": 2}),
        ({"key": 0}, {"key": None}),
    ],
)
def test_duplicate_key_raises(origin: dict[str, int], update: dict[str, Any]) -> None:
    with pytest.raises(KeyError) as exc:
        dict_merge_update_into_origin(origin, update)
    assert "already exists in origin" in str(exc.value)


@pytest.mark.parametrize(
    ("origin", "update", "error_type"),
    [
        (None, {}, TypeError),
        ([], {"a": 1}, TypeError),
        ({"a": 1}, None, TypeError),
        ({"a": 1}, [], TypeError),
    ],
)
def test_type_errors(origin: Any, update: Any, error_type: type) -> None:
    with pytest.raises(error_type):  # noqa
        dict_merge_update_into_origin(origin, update)
