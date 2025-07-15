"""

    Environment

"""

import logging
import os

from alsek.defaults import ALSEK_WORKER_POOL_ENV_VAR

log = logging.getLogger(__name__)


def set_alsek_worker_pool_env_var() -> None:
    if ALSEK_WORKER_POOL_ENV_VAR in os.environ:
        log.warning("Worker thread env var already set.")
    else:
        os.environ[ALSEK_WORKER_POOL_ENV_VAR] = "true"
