"""

    Test Helpers

"""
import time
from typing import Any, Callable


def expand_params_factory(
    expander: tuple[Any, ...],
) -> Callable[..., list[tuple[Any, ...]]]:
    """Factory to generate a function which can expand testing parameters.

    Args:
        expander (tuple[Any]): a tuple of values to expand ``params`` by

    Returns:
        engine (callable): engine to add all items in ``expander`` to ``params``.

    """

    def engine(*params: Any) -> list[tuple[Any, ...]]:
        expanded = list()
        for s in expander:
            for p in params:
                if isinstance(p, (list, tuple)):
                    expanded.append((*p, s))
                else:
                    expanded.append((p, s))
        return expanded

    return engine


def sleeper(milliseconds: int, buffer: int = 100) -> None:
    """Delay execution.

    Args:
        milliseconds (int): amount of time to delay for
             in milliseconds.
        buffer (int): buffer ontop of ``seconds``
            in milliseconds

    Returns:
        None

    """
    time.sleep((milliseconds + buffer) / 1000)
