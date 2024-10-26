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


def test_parsed_exception_raising() -> None:
    try:
        raise ZeroDivisionError("division by zero")
    except ZeroDivisionError as error:
        details = parse_exception(error)

    with pytest.raises(ZeroDivisionError):
        details.raise_as_exception()


def test_parsed_exception_raising_when_import_required() -> None:
    details = ExceptionDetails(
        name="tests.assets.exceptions.AlsekTestException",
        text="<Message Here>",
        traceback="",
    )

    try:
        details.raise_as_exception()
    except BaseException as error:
        # Delay the import until after invocation of `raise_as_exception()`
        from tests.assets.exceptions import AlsekTestException

        with pytest.raises(AlsekTestException):
            raise error
