"""

    Exception Parsing Test

"""
import pytest
from alsek.utils.parsing import extract_exception_details, ExceptionDetails


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
def test_extract_exception_details(
    exception: Exception,
    expected_name: str,
    expected_text: str,
) -> None:
    details = extract_exception_details(exception)

    assert isinstance(details, ExceptionDetails)
    assert details.name == expected_name
    assert details.text == expected_text
    assert expected_text in details.traceback
    assert expected_name in details.traceback
