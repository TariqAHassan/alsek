"""

    Aggregation Utils

"""
import inspect
from typing import Any, Optional, Tuple, Dict


def gather_init_params(
    obj: Any,
    ignore: Optional[Tuple[str, ...]] = None,
) -> Dict[str, Any]:
    """Extract the parameters passed to an object's ``__init__()``.

    Args:
        obj (object):
        ignore (tuple, optional): parameters in ``__init__()`` to ignore

    Returns:
        params (dict): parameters from ``__init__()``.

    """
    params = dict()
    for k in inspect.signature(obj.__init__).parameters:
        if ignore and k in ignore:
            continue
        params[k] = getattr(obj, k)
    return params
