from pathlib import Path

import pytest

from pg2pyrquet.core.exceptions import (
    DirectoryDoesNotExistError,
    DirectoryIsAFileError,
    QueryFileDoesNotExistError,
    QueryFileIsADirectoryError,
)
from pg2pyrquet.utils.path import validate_output_path, validate_query_path


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


def test_validate_query_path_valid():
    query_path = Path("./test_query.sql")
    query_path.touch()
    assert validate_query_path(query_path=query_path) == query_path
    query_path.unlink()


def test_validate_query_path_non_existent():
    query_path = Path("./non_existent_query.sql")
    with pytest.raises(QueryFileDoesNotExistError):
        validate_query_path(query_path=query_path)


def test_validate_query_path_is_directory():
    query_path = Path("./test_directory")
    query_path.mkdir()
    with pytest.raises(QueryFileIsADirectoryError):
        validate_query_path(query_path=query_path)
    query_path.rmdir()
