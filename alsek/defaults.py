"""

    Defaults

"""

from alsek.types import SupportedMechanismType

DEFAULT_QUEUE: str = "alsek_queue"
DEFAULT_NAMESPACE: str = "alsek"
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_TASK_TIMEOUT: int = 60 * 60 * 1000
DEFAULT_MECHANISM: SupportedMechanismType = "thread"
ALSEK_WORKER_POOL_ENV_VAR: str = "ALSEK_WORKER_POOL"

DEFAULT_TTL: int = 60 * 60 * 24 * 7 * 1000  # 1 week in ms
