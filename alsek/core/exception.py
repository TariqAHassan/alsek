"""

    Exception Parsing

"""
import traceback
from typing import NamedTuple


class ExceptionDetails(NamedTuple):
    name: str
    text: str
    traceback: str


def extract_exception_details(error: Exception) -> ExceptionDetails:
    """Extracts the exception type, exception message, and exception
    traceback from an error.

    Args:
        error (Exception): The exception to extract details from.

    Returns:
        details (ExceptionDetails): A named tuple containing the exception information

    """
    return ExceptionDetails(
        name=type(error).__name__,
        text=str(error),
        traceback="".join(
            traceback.format_exception(
                etype=type(error),
                value=error,
                tb=error.__traceback__,
            )
        ),
    )


if __name__ == "__main__":
    try:
        1 / 0
    except Exception as e:
        details = extract_exception_details(e)
        print("Exception Type:", details.name)
        print("Exception Message:", details.text)
        print("Exception Traceback:", details.traceback)
