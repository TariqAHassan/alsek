"""

    Exception Parsing Test

"""

import pytest

from alsek.utils.parsing import ExceptionDetails, parse_exception


@pytest.mark.parametrize(
    "exception,expected_name,expected_text",
    [
        (
            ZeroDivisionError("division by zero"),
            "ZeroDivisionError",
            "division by zero",
        ),
        (
            ValueError("invalid value"),
            "ValueError",
            "invalid value",
        ),
        (
            TypeError("type error"),
            "TypeError",
            "type error",
        ),
    ],
)
def test_parse_exception(
    exception: Exception,
    expected_name: str,
    expected_text: str,
) -> None:
    details = parse_exception(exception)

    assert isinstance(details, ExceptionDetails)
    assert details.name == expected_name
    assert details.text == expected_text
    assert expected_text in details.traceback
    assert expected_name in details.traceback
