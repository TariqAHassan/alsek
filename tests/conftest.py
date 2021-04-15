"""

    Conftest

"""
import pytest
from alsek.storage.backends.disk import DiskCacheBackend


@pytest.fixture()
def disk_cache_backend() -> DiskCacheBackend:
    dsb = DiskCacheBackend()
    yield dsb
    dsb.destroy()
