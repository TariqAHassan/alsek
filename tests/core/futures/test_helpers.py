"""

    Test Future Helpers

"""
import pytest
from alsek.core.futures import (
    _generate_callback_message,
    _process_future_encoder,
    _process_future_decoder,
    _retry_future_handler,
    _complete_future_handler,
)
from alsek.core.task import task
from alsek.core.message import Message
from alsek.core.broker import Broker
from alsek._defaults import DEFAULT_MAX_RETRIES
from alsek.storage.result import ResultStore


def test_generate_callback_message() -> None:
    message = Message("task")
    result = _generate_callback_message(
        callback_message_data=message.data,
        previous_result=None,
        progenitor=None,  # Note: this would not be `None` in practice.
    )
    assert result.data == message.data


def test_process_furture_encoding(rolling_broker: Broker) -> None:
    testing_task = task(rolling_broker)(lambda: 1)
    testing_msg = testing_task.generate()

    encoded = _process_future_encoder(testing_task, message=testing_msg)
    decoded_testing_task, decoded_testing_msg = _process_future_decoder(encoded)

    # Check that the message was reconstructed
    assert testing_msg.data == decoded_testing_msg.data

    # Check that the reconstructed task is similar in kind
    assert testing_task.name == decoded_testing_task.name
    assert testing_task.broker.__class__ == decoded_testing_task.broker.__class__
    assert testing_task.broker.dlq_ttl == decoded_testing_task.broker.dlq_ttl
    assert (
        testing_task.broker.backend.__class__
        == decoded_testing_task.broker.backend.__class__
    )


@pytest.mark.parametrize("retries", [0, DEFAULT_MAX_RETRIES])
def test_retry_future_handler(
    retries: int,
    rolling_broker: Broker,
) -> None:
    testing_task = task(rolling_broker)(lambda: 1).defer()
    testing_msg = testing_task.generate().update(retries=retries)
    rolling_broker.submit(testing_msg)

    _retry_future_handler(testing_task, message=testing_msg, exception=BaseException())
    if retries < DEFAULT_MAX_RETRIES:
        assert testing_msg.retries == 1
    else:
        assert not rolling_broker.exists(testing_msg)


def test_complete_future_handler(
    rolling_broker: Broker,
    rolling_result_store: ResultStore,
) -> None:
    testing_task = task(rolling_broker, result_store=rolling_result_store)(lambda: 1)
    testing_msg = testing_task.generate()

    _complete_future_handler(testing_task, message=testing_msg, result=1)
    assert not rolling_broker.exists(testing_msg)


def test_complete_future_handler_store_result(
    rolling_broker: Broker,
    rolling_result_store: ResultStore,
) -> None:
    testing_task = task(rolling_broker, result_store=rolling_result_store)(lambda: 1)
    testing_msg = testing_task.generate()

    _complete_future_handler(testing_task, message=testing_msg, result=1)
    assert rolling_result_store.exists(testing_msg)


def test_complete_future_handler_callback(
    rolling_broker: Broker,
    rolling_result_store: ResultStore,
) -> None:
    testing_task = task(rolling_broker, result_store=rolling_result_store)(lambda: 1)
    callback_msg = Message("task")
    testing_msg = testing_task.generate().update(
        callback_message_data=callback_msg.data
    )

    _complete_future_handler(testing_task, message=testing_msg, result=1)
    assert rolling_broker.exists(callback_msg)
