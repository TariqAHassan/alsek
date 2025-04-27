"""

    Core

"""

import os
import logging

log = logging.getLogger()

MULTIPROCESSING_BACKEND = os.getenv("ALSEK_MULTIPROCESSING_BACKEND", "standard").strip()

if MULTIPROCESSING_BACKEND == "standard":
    from multiprocessing import Event, Process, Queue

    log.info("Using standard multiprocessing backend.")
elif MULTIPROCESSING_BACKEND == "torch":
    from torch.multiprocessing import Event, Process, Queue  # noqa

    log.info("Using torch multiprocessing backend.")
else:
    raise ImportError(f"Invalid multiprocessing backend '{MULTIPROCESSING_BACKEND}'")
