import re

import pytest
from typer.testing import CliRunner

from pg2pyrquet.__main__ import app

runner = CliRunner()

COMMANDS = ["export-database", "export-table", "export-query"]

ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def render_help(*args: str) -> str:
    """
    Renders the help output for the given arguments without ANSI codes.

    Rich splits styled words with escape sequences, so the raw output is not
    searchable when colours are enabled.
    """
    result = runner.invoke(app, [*args, "--help"])

    assert result.exit_code == 0, result.output

    return ANSI_ESCAPE_PATTERN.sub("", result.output)


def test_cli_help_lists_every_command():
    output = render_help()

    for command in COMMANDS:
        assert command in output


@pytest.mark.parametrize("command", COMMANDS)
def test_command_help_renders_options(command):
    output = render_help(command)

    assert "--database" in output
    assert "--batch-size" in output
