"""
Module configuration custom logger.
"""

import logging
from typing import Any

DEFAULT_LOGGER_NAME = "postgres-to-parquet-dumps"

LOG_MESSAGE_FORMAT = "[%(name)s] [%(asctime)s] %(message)s"
LOG_DATE_FORMAT = (
    "%Y-%m-%dT%H:%M:%S"  # Corrected date format to include seconds
)


class CustomHandler(logging.StreamHandler):
    """
    Custom logging handler that formats log messages using a specific format.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        formatter = logging.Formatter(
            fmt=LOG_MESSAGE_FORMAT, datefmt=LOG_DATE_FORMAT
        )
        self.setFormatter(formatter)


def get_logger(
    name: str = DEFAULT_LOGGER_NAME, level: int = logging.INFO
) -> logging.Logger:
    """
    Configures and returns a logger with the specified name and logging level.

    Args:
        name (str): The name of the logger. Defaults to DEFAULT_LOGGER_NAME.
        level (int): The logging level. Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(CustomHandler())
    return logger
