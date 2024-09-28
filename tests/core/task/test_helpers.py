"""

    Test Task Helpers

"""

from typing import Any, Callable

import pytest

from alsek.core.broker import Broker
from alsek.core.message import Message
from alsek.core.task import (
    _collapse_callbacks,
    _expects_message,
    _MultiSubmit,
    _parse_callback,
    task,
)


def _func_with_message_1(message) -> None:  # type: ignore
    pass


def _func_with_message_2(message: Message) -> None:  # type: ignore
    pass


def _func_without_message_1() -> None:  # type: ignore
    pass


def _func_without_message_2(other) -> None:  # type: ignore
    pass


def _func_without_message_3(other: float) -> None:  # type: ignore
    pass


def _func_without_message_4(message: str) -> None:  # type: ignore
    pass


@pytest.mark.parametrize(
    "func,expected",
    [
        (_func_with_message_1, True),
        (_func_with_message_2, True),
        (_func_without_message_1, False),
        (_func_without_message_2, False),
        (_func_without_message_3, False),
        (_func_without_message_4, False),
    ],
)
def test_expects_message(func: Callable[..., Any], expected: bool) -> None:
    assert _expects_message(func) is expected


def test_collapse_callbacks(rolling_broker: Broker) -> None:
    @task(rolling_broker)
    def add_1_prev(message: Message) -> int:
        return message.previous_result + 1

    callbacks = [add_1_prev.defer().generate() for _ in range(3)]

    collapsed = _collapse_callbacks(callbacks)
    assert collapsed.uuid == callbacks[0].uuid
    assert collapsed.callback_message_data["uuid"] == callbacks[1].uuid
    assert (
        collapsed.callback_message_data["callback_message_data"]["uuid"]
        == callbacks[2].uuid
    )


@pytest.mark.parametrize("count", range(1, 3))
def test_parse_callback(count: int, rolling_broker: Broker) -> None:
    @task(rolling_broker)
    def add_1_prev(message: Message) -> int:
        return message.previous_result + 1

    callbacks = [add_1_prev.defer().generate() for _ in range(count)]
    assert _parse_callback(callbacks) == callbacks[0]


def test_multi_submit(rolling_broker: Broker) -> None:
    # Instantiate `_MultiSubmit()` against a message
    message = Message("task")
    multi_submit = _MultiSubmit(message, broker=rolling_broker)

    # Check the `first` flag starts as `True`
    assert multi_submit.first

    # Submit
    multi_submit()

    # Check the `first` flag is now `False`
    assert not multi_submit.first

    # Check that the message was submitted by the broker
    assert rolling_broker.exists(message)
