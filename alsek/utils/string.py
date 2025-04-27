"""

    String Utils

"""
from typing import Optional


def smart_join(
    items: list[str],
    limit: Optional[int] = 5,
    delimiter: str = ", ",
) -> str:
    """Joins a list of strings with a delimiter, limiting the number of items to display
    and optionally appending a continuation indicator or providing a grammatically
    correct conjunction for the last two items.

    Args:
        items (list[str]): A list of strings to be joined.
        limit (Optional[int]): The maximum number of items to include in the joined
            string. If None, join all items without limiting.
        delimiter (str): The string used to separate the items in the joined output.

    Returns:
        str: A string containing the joined items, formatted according to the
        specified delimiter and limits.

    Raises:
        ValueError: If the `items` list is empty.

    """
    if len(items) == 0:
        raise ValueError("No items to join")
    elif len(items) == 1:
        return items[0]
    elif limit is None or len(items) <= limit:
        *start, last = items
        return f"{delimiter.join(start)} and {last}"
    else:
        return delimiter.join(items[:limit]) + "..."
