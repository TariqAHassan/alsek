"""

    Test Iteration
    ~~~~~~~~~~~~~~

"""
import copy
import random
from typing import Any, List, Optional, Set

import pytest

from alsek.core.message import Message
from alsek.exceptions import ValidationError
from alsek.tools.iteration import ResultPool, _idx_drop


def _shuffle(l: List[Any], seed: int = 42) -> List[Any]:
    random.seed(seed)
    l = copy.deepcopy(l)
    random.shuffle(l)
    return l


@pytest.mark.parametrize(
    "items,indexes,expected",
    [
        (["a", "b", "c"], {}, ["a", "b", "c"]),
        (["a", "b", "c"], {0, 1, 2}, []),
        (["a", "b", "c"], {0}, ["b", "c"]),
        (["a", "b", "c"], {1}, ["a", "c"]),
        (["a", "b", "c"], {2}, ["a", "b"]),
    ],
)
def test_idx_drop(items: List[Any], indexes: Set[int], expected: List[Any]) -> None:
    assert _idx_drop(items, indexes=indexes) == expected


@pytest.mark.parametrize(
    "messages,expected",
    [
        # Problem: none
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="b"),
            ],
            None,
        ),
        # Problem: Duplicate UUIDs
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="a"),
            ],
            ValidationError,
        ),
    ],
)
def test_validation(
    messages: List[Message],
    expected: Optional[Exception],
    rolling_result_pool: ResultPool,
) -> None:
    if expected is None:
        rolling_result_pool._validate(messages)
    else:
        with pytest.raises(expected):
            rolling_result_pool._validate(messages)


@pytest.mark.parametrize(
    "messages",
    [
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="b"),
            ]
        ),
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="b"),
                Message("task", uuid="c"),
                Message("task", uuid="d"),
            ]
        ),
    ],
)
def test_istream(
    messages: List[Message],
    rolling_result_pool: ResultPool,
) -> None:
    expected_msgs = set(messages)
    expected_results = {1}

    for m in _shuffle(messages):
        rolling_result_pool.result_store.set(m, result=1)

    actual_uuids, actual_results = set(), set()
    for uuid, result in rolling_result_pool.istream(*messages):
        actual_uuids.add(uuid)
        actual_results.add(result)

    # Check that the all results for all messages have been returned.
    assert expected_msgs == actual_uuids
    assert expected_results == actual_results


@pytest.mark.parametrize(
    "messages",
    [
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="b"),
            ]
        ),
        (
            [
                Message("task", uuid="a"),
                Message("task", uuid="b"),
                Message("task", uuid="c"),
                Message("task", uuid="d"),
            ]
        ),
    ],
)
def test_stream(
    messages: List[Message],
    rolling_result_pool: ResultPool,
) -> None:
    expected_msgs = messages
    expected_results = [1] * len(messages)

    for m in _shuffle(messages):
        rolling_result_pool.result_store.set(m, result=1)

    actual_uuids, actual_results = list(), list()
    for uuid, result in rolling_result_pool.stream(*messages):
        actual_uuids.append(uuid)
        actual_results.append(result)

    # Check that:
    #   * all results for all messages have been returned
    #   * the output order of the results matches the input order
    assert expected_msgs == actual_uuids
    assert expected_results == actual_results
