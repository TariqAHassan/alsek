"""

    Singletons

"""

from pathlib import Path

from alsek import Broker
from alsek.storage.backends.disk.standard import DiskCacheBackend

cache_dir = Path("~/alsek/diskcache").expanduser()
cache_dir.mkdir(parents=True, exist_ok=True)

backend = DiskCacheBackend(cache_dir)
broker = Broker(backend)
