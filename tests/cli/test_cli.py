"""

    Test CLI

    References:
        * https://click.palletsprojects.com/en/7.x/testing/

    ToDo: expand cli testing

"""

from __future__ import annotations

from typing import Any

from click.testing import CliRunner
from pytest_mock import MockFixture

from alsek import __version__


def test_version(
    cli_runner: CliRunner,
    mocker: MockFixture,
) -> None:
    result = cli_runner.invoke(args=["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_help(
    cli_runner: CliRunner,
    mocker: MockFixture,
) -> None:
    result = cli_runner.invoke(args=["--help"])
    assert result.exit_code == 0
    assert "Alsek" in result.output
