"""

    Decorators

"""

from functools import wraps
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def exception_suppressor(*exceptions: type[BaseException]) -> Callable[[F], F]:
    def decorator(func: F) -> F:  # type: ignore[arg-type]
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except exceptions:
                return None

        return wrapper

    return decorator
