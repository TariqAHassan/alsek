"""

    Helpers

"""

from copy import deepcopy
from typing import Any, Optional


def dict_merge_update_into_origin(
    origin: dict[Any, Any],
    update: dict[Any, Any],
    inplace: bool = False,
) -> dict[str, Any]:
    if not isinstance(origin, dict):
        raise TypeError("origin must be a dictionary")
    elif not isinstance(update, dict):
        raise TypeError("update must be a dictionary")

    if not inplace:
        origin = deepcopy(origin)

    for k, v in update.items():
        if k in origin:
            raise KeyError(f"key '{k}' already exists in origin")
        origin[k] = v
    return origin
