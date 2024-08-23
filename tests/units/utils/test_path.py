from pathlib import Path

import pytest

from pg2pyrquet.core.exceptions import (
    DirectoryDoesNotExistError,
    DirectoryIsAFileError,
)
from pg2pyrquet.utils.path import validate_output_path


def test_validate_output_path_existing_directory():
    path = Path("./existing")
    path.mkdir(parents=True, exist_ok=True)
    assert validate_output_path(output_path=path) == path
    path.rmdir()


def test_validate_output_path_non_existing_directory():
    path = Path("./non/existing/directory")
    with pytest.raises(DirectoryDoesNotExistError):
        validate_output_path(output_path=path)


def test_validate_output_path_is_file():
    file = Path("./file.txt")
    file.touch()
    with pytest.raises(DirectoryIsAFileError):
        validate_output_path(output_path=file)
    file.unlink()
