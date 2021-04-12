"""

    Sorting

"""
from typing import Any, Callable, Dict


def dict_sort(
    dictionary: Dict[Any, Any],
    key: Callable[[Any], Any] = lambda k: k,
) -> Dict[Any, Any]:
    """Sort a dictionary by key.

    Args:
        dictionary:
        key (callable): a callable which consumes a key
            and returns an object which supports the
            less than comparison operator.

    Returns:
        sorted_dictionary (dict): ``dictionary`` sorted

    """
    return dict(sorted(dictionary.items(), key=lambda x: key(x[0])))
