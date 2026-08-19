import pytest
import typer

from pg2pyrquet.core.errors import handle_cli_errors
from pg2pyrquet.core.exceptions import DirectoryDoesNotExistError


def test_expected_error_becomes_a_clean_exit():
    @handle_cli_errors
    def command() -> None:
        raise DirectoryDoesNotExistError("Output directory is missing.")

    with pytest.raises(typer.Exit) as error:
        command()

    assert error.value.exit_code == 1


def test_expected_error_is_logged(caplog):
    @handle_cli_errors
    def command() -> None:
        raise DirectoryDoesNotExistError("Output directory is missing.")

    with pytest.raises(typer.Exit):
        command()

    assert "Output directory is missing." in caplog.text


def test_unexpected_error_keeps_its_traceback():
    @handle_cli_errors
    def command() -> None:
        raise ZeroDivisionError("this is a real bug")

    with pytest.raises(ZeroDivisionError):
        command()


def test_return_value_passes_through():
    @handle_cli_errors
    def command() -> str:
        return "done"

    assert command() == "done"
