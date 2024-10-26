"""

    Parsing

"""

import traceback
from typing import NamedTuple, Optional


class ExceptionDetails(NamedTuple):
    name: str
    text: Optional[str] = None
    traceback: Optional[str] = None

    def as_dict(self) -> dict[str, str]:
        return self._asdict()


def parse_exception(error: Exception) -> ExceptionDetails:
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
                type(error),
                value=error,
                tb=error.__traceback__,
            )
        ),
    )
