"""

    Test Base

"""
import pytest
from diskcache import Cache as DiskCache

from alsek._defaults import DEFAULT_NAMESPACE
from alsek.storage.backends import Backend, LazyClient


def test_lazy_client() -> None:
    lazy_client = LazyClient(lambda: DiskCache())
    assert isinstance(lazy_client.get(), DiskCache)


def test_repr(base_backend: Backend) -> None:
    assert isinstance(repr(base_backend), str)


@pytest.mark.parametrize(
    "name,expected",
    [
        (f"{DEFAULT_NAMESPACE}:", True),
        (f"{DEFAULT_NAMESPACE}:a", True),
        (f"{DEFAULT_NAMESPACE}:a:b", True),
        (f"{DEFAULT_NAMESPACE}", False),  # invalid without ending in ":"
        ("a", False),
        ("b", False),
        (f"a:{DEFAULT_NAMESPACE}", False),
        (f"b:{DEFAULT_NAMESPACE}", False),
    ],
)
def test_in_namespace(
    name: str,
    expected: bool,
    base_backend: Backend,
) -> None:
    assert base_backend.in_namespace(name) is expected


@pytest.mark.parametrize(
    "name,expected",
    [
        (f"{DEFAULT_NAMESPACE}:", f"{DEFAULT_NAMESPACE}:"),
        ("b", f"{DEFAULT_NAMESPACE}:b"),
    ],
)
def test_full_name(
    name: str,
    expected: str,
    base_backend: Backend,
) -> None:
    assert base_backend.full_name(name) == expected


@pytest.mark.parametrize(
    "name,expected",
    [
        (f"{DEFAULT_NAMESPACE}:b", "b"),
        (f"{DEFAULT_NAMESPACE}:b:c:d", "b:c:d"),
    ],
)
def test_short_name(
    name: str,
    expected: str,
    base_backend: Backend,
) -> None:
    assert base_backend.short_name(name) == expected
