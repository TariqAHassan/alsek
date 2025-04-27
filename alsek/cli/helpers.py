"""

    Helpers

"""

from importlib.util import find_spec
from pathlib import Path


def package2path(package: str) -> Path:
    """Convert a Python package name into its corresponding filesystem path."""
    spec = find_spec(package)
    if spec is None or spec.origin is None:
        raise ModuleNotFoundError(f"Package '{package}' not found.")

    path = Path(spec.origin)
    return path.parent if path.name == "__init__.py" else path
