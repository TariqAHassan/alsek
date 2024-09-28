"""

    Defaults

"""

from alsek.types import SupportedMechanismType

DEFAULT_QUEUE: str = "alsek_queue"
DEFAULT_NAMESPACE: str = "alsek"
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_TASK_TIMEOUT: int = 60 * 60 * 1000
DEFAULT_MECHANISM: SupportedMechanismType = "process"

DEFAULT_TTL: int = 60 * 60 * 24 * 7 * 1000
