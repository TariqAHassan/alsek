"""

    Check Utils

"""
from typing import Collection, Any


def has_duplicates(itera: Collection[Any]) -> bool:
    """Determine if ``itera`` contains duplicates.

    Args:
        itera (Collection): a sized iterable

    Returns:
        bool

    """
    seen = set()
    for i in itera:
        if i in seen:
            return True
        seen.add(i)
    return False
