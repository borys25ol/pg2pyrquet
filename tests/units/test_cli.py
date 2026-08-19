import pytest
from typer.testing import CliRunner

from pg2pyrquet.__main__ import app

runner = CliRunner()

COMMANDS = ["export-database", "export-table", "export-query"]


def test_cli_help_lists_every_command():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    for command in COMMANDS:
        assert command in result.output


@pytest.mark.parametrize("command", COMMANDS)
def test_command_help_renders(command):
    result = runner.invoke(app, [command, "--help"])

    assert result.exit_code == 0, result.output
    assert "--database" in result.output
