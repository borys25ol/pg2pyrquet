import re
from importlib.metadata import version

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
    assert "--batch-size-bytes" in output
    assert "--row-group-size" in output
    assert "--batch-size " not in output


@pytest.mark.parametrize("command", COMMANDS)
def test_host_and_port_have_defaults(command):
    output = render_help(command)

    assert "localhost" in output
    assert "5432" in output


@pytest.mark.parametrize("command", COMMANDS)
def test_dsn_option_is_offered(command):
    assert "--dsn" in render_help(command)


def test_database_is_required_without_a_dsn(caplog):
    result = runner.invoke(
        app, ["export-table", "--table", "t", "--folder", "."]
    )

    assert result.exit_code == 1
    assert "--database" in caplog.text


@pytest.mark.parametrize("command", ["export-database", "export-table"])
def test_schema_option_is_offered(command):
    assert "--schema" in render_help(command)


@pytest.mark.parametrize("command", COMMANDS)
def test_overwrite_option_is_offered(command):
    assert "--overwrite" in render_help(command)


def test_continue_on_error_is_offered_for_the_database_command():
    assert "--continue-on-error" in render_help("export-database")


def test_version_is_printed():
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert ANSI_ESCAPE_PATTERN.sub("", result.output).strip() == version(
        "pg2pyrquet"
    )
