import ast
import os
import sys
from importlib.util import find_spec
from pathlib import Path
from typing import Callable

import click
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

WATCHED_FILE_EXTENSIONS = (".py",)


def package2path(package: str) -> Path:
    """Convert a Python package name into its corresponding filesystem path."""
    spec = find_spec(package)
    if spec is None or spec.origin is None:
        raise ModuleNotFoundError(f"Package '{package}' not found.")

    path = Path(spec.origin)
    return path.parent if path.name == "__init__.py" else path


def configure_reload(package: str) -> Observer:
    sys.path.append(os.getcwd())
    directory = package2path(package)
    if not directory.is_dir():
        raise NotADirectoryError(f"The provided path '{str(directory)}' is not a directory")  # fmt: skip

    def restart_program() -> None:
        os.execv(sys.executable, [sys.executable] + sys.argv)

    observer = Observer()
    handler = RestartOnChangeHandler(restart_callback=restart_program)
    observer.schedule(handler, path=str(directory), recursive=True)
    observer.start()
    return observer


class RestartOnChangeHandler(FileSystemEventHandler):
    def __init__(self, restart_callback: Callable[[], None]) -> None:
        self.restart_callback = restart_callback

    @staticmethod
    def _has_valid_syntax(file_path: str) -> bool:
        try:
            source = Path(file_path).open("r", encoding="utf-8").read()
            ast.parse(source, filename=file_path)
            return True
        except SyntaxError:
            return False

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory and event.src_path.endswith(WATCHED_FILE_EXTENSIONS):
            if self._has_valid_syntax(event.src_path):
                click.echo(f"Detected changes in {event.src_path}. Restarting worker pool...")  # fmt: skip
                self.restart_callback()
            else:
                click.echo(f"Skipping restart due to syntax errors in {event.src_path}...")  # fmt: skip
