"""

    Test Helpers

"""
import time
from typing import Any, Callable, List, Tuple


def expand_params_factory(
    expander: Tuple[Any, ...],
) -> Callable[..., List[Tuple[Any, ...]]]:
    """Factory to generate a function which can expand testing parameters.

    Args:
        expander (Tuple[Any]): a tuple of values to expand ``params`` by

    Returns:
        engine (callable): engine to add all items in ``expander`` to ``params``.

    """

    def engine(*params: Any) -> List[Tuple[Any, ...]]:
        expanded = list()
        for s in expander:
            for p in params:
                if isinstance(p, (list, tuple)):
                    expanded.append((*p, s))
                else:
                    expanded.append((p, s))
        return expanded

    return engine


def sleeper(seconds: int, buffer: int = 100) -> None:
    """Delay execution.

    Args:
        seconds (int): amount of time to delay for
             in milliseconds.
        buffer (int): buffer ontop of ``seconds``
            in milliseconds

    Returns:
        None

    """
    time.sleep((seconds + buffer) / 1000)
