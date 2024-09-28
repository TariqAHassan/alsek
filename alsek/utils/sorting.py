"""

    Sorting

"""

from typing import Any, Callable, Optional


def dict_sort(
    dictionary: dict[Any, Any],
    key: Optional[Callable[[Any], Any]] = None,
) -> dict[Any, Any]:
    """Sort a dictionary by key.

    Args:
        dictionary:
        key (callable): a callable which consumes a key
            and returns an object which supports the
            less than comparison operator.

    Returns:
        sorted_dictionary (dict): ``dictionary`` sorted

    """
    return dict(sorted(dictionary.items(), key=lambda x: (key or (lambda k: k))(x[0])))  # type: ignore
