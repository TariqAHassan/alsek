"""

    String Utils
    ~~~~~~~~~~~~~

"""
from typing import Optional


def name_matcher(pattern: Optional[str], name: str) -> bool:
    """Determine if ``pattern`` matches ``name``.

    This function supports the following forms
    of matching:

        * none (all names considered to be matches)
        * direct (pattern == name)
        * leading astrix (e.g., "*pattern") for tail matching
        * trailing astrix (e.g., "pattern*") for head matching
        * leading and trailing astrix (e.g., "*pattern*") for substring matching

    Args:
        pattern (str, optional): the pattern to match against
        name (str): the target name.

    Returns:
        bool

    Examples:
        >>> assert name_matcher(None, name="cats")
        >>> assert name_matcher("cats", name="cats")
        >>> assert name_matcher("*cats", name="happy_cats")
        >>> assert name_matcher("cats*", name="cats_happy")
        >>> assert name_matcher("*cats*", name="many cats happy")

    """
    if pattern is None:
        return True
    elif pattern == name:
        return True
    elif pattern.startswith("*") and pattern.endswith("*"):
        return pattern.strip("*") in name
    elif pattern.startswith("*"):
        return name.endswith(pattern.lstrip("*"))
    elif pattern.endswith("*"):
        return name.startswith(pattern.rstrip("*"))
    else:
        return False
