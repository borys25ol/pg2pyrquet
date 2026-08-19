"""
CLI error handling.

Expected failures reach the user as a single line and a non-zero exit
code. Anything unexpected keeps its traceback, because hiding a real bug
is worse than printing a stack trace.
"""

import functools
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
import typer
from adbc_driver_manager import Error as AdbcError

from pg2pyrquet.core.exceptions import Pg2ParquetError
from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)

ReturnType = TypeVar("ReturnType")

# Failures the user can act on, reported without a traceback
EXPECTED_ERRORS = (AdbcError, OSError, Pg2ParquetError, psycopg.Error)


def handle_cli_errors(
    command: Callable[..., ReturnType],
) -> Callable[..., ReturnType]:
    """
    Turns an expected failure into a logged message and exit code 1.

    Args:
        command (Callable): The command function to wrap.

    Returns:
        Callable: The wrapped command.
    """

    @functools.wraps(command)
    def wrapper(*args: Any, **kwargs: Any) -> ReturnType:
        try:
            return command(*args, **kwargs)
        except EXPECTED_ERRORS as error:
            logger.error(str(error))
            raise typer.Exit(code=1) from error

    return wrapper
