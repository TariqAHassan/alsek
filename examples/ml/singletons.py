"""

    Singletons

"""
from pathlib import Path

from alsek.core import Broker
from alsek.storage.backends.disk import DiskCacheBackend
from alsek.storage.result import ResultStore

cache_dir = Path("~/alsek/diskcache").expanduser()
cache_dir.mkdir(parents=True, exist_ok=True)

backend = DiskCacheBackend(cache_dir)
broker = Broker(backend)
result_store = ResultStore(backend)
