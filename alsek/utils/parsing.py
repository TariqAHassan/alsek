"""

    Parsing

"""

import traceback
from typing import NamedTuple, Optional
from types import SimpleNamespace
from importlib import import_module


class ExceptionDetails(NamedTuple):
    name: str
    text: Optional[str] = None
    traceback: Optional[str] = None

    def as_dict(self) -> dict[str, str]:
        return self._asdict()

    def raise_as_exception(self) -> None:
        # Rather than Exception, let import the actual error with importlib
        # if it's not builtin.
        if "." in str(self.name):
            module_name, exception_name = self.name.rsplit(".", 1)
            exec_class = getattr(import_module(module_name), exception_name)
        else:
            exec_class = getattr(__builtins__, self.name)
        raise exec_class(self.text)


def parse_exception(error: BaseException) -> ExceptionDetails:
    """Extracts the exception type, exception message, and exception
    traceback from an error.

    Args:
        error (BaseException): The exception to extract details from.

    Returns:
        details (ExceptionDetails): A named tuple containing the exception information

    """
    qualname, module = type(error).__module__, type(error).__qualname__
    return ExceptionDetails(
        name=module if qualname == "builtins" else f"{qualname}.{module}",
        text=str(error),
        traceback="".join(
            traceback.format_exception(
                type(error),
                value=error,
                tb=error.__traceback__,
            )
        ),
    )
