"""

    Parsing

"""

import traceback
import builtins
from typing import NamedTuple, Optional, Type
from importlib import import_module


def _get_exception_class(name: str) -> Type[BaseException]:
    if "." in str(name):
        module_name, exception_name = name.rsplit(".", 1)
        exec_class = getattr(import_module(module_name), exception_name)
    else:
        exec_class = getattr(builtins, name)
    return exec_class


class ExceptionDetails(NamedTuple):
    name: str
    text: Optional[str] = None
    traceback: Optional[str] = None

    def as_dict(self) -> dict[str, str]:
        """Convert the NamedTuple to a dictionary

        Returns:
            dict

        """
        return self._asdict()

    def raise_as_exception(self, strict: bool = True) -> None:
        """Raise the parsed exception information as a Python exception.

        Args:
            strict (bool): if ``True`` do no coerce failures to
                import the correct error

        Returns:
            None

        Warnings:
            This will not include the original traceback.

        """
        try:
            exc, msg = _get_exception_class(self.name), self.text
        except (ImportError, AttributeError) as error:
            if strict:
                raise error
            else:
                exc, msg = Exception, f"{self.name}: {self.text}"
        raise exc(msg)


def parse_exception(error: BaseException) -> ExceptionDetails:
    """Extracts the exception type, exception message, and exception
    traceback from an error.

    Args:
        error (BaseException): The exception to extract details from.

    Returns:
        details (ExceptionDetails): A named tuple containing the exception information

    """
    module, qualname = type(error).__module__, type(error).__qualname__
    return ExceptionDetails(
        name=qualname if module == "builtins" else f"{module}.{qualname}",
        text=str(error),
        traceback="".join(
            traceback.format_exception(
                type(error),
                value=error,
                tb=error.__traceback__,
            )
        ),
    )
