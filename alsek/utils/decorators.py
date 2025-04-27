"""

    Decorators

"""

from functools import wraps
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def suppress_exception(
    *exceptions: type[BaseException],
    on_suppress: Callable[[BaseException], bool] = lambda e: None,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:  # type: ignore[arg-type]
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except exceptions as error:
                on_suppress(error)
                return None

        return wrapper

    return decorator
