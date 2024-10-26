"""

    Types

"""

from typing import Literal

SUPPORTED_MECHANISMS: tuple[str, ...] = ("process", "thread")
SupportedMechanismType = Literal["process", "thread"]


class Empty:
    """Empty sentinel."""
