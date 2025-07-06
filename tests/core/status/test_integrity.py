"""

    Test Integrity

"""
import pytest
from alsek.core.status.integrity import _name2message  # noqa


@pytest.mark.parametrize(
    "name,expected",
    [
        ("status:queue1:task1:uuid1", ("task1", "queue1", "uuid1")),
        ("namespace:status:queue2:task2:uuid2", ("task2", "queue2", "uuid2")),
    ],
)
def test_name2message(name, expected: tuple[str, str, str]) -> None:
    message = _name2message(name)
    actual = (message.task_name, message.queue, message.uuid)
    assert actual == expected
