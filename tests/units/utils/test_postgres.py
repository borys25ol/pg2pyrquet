from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pg2pyrquet.core.exceptions import (
    DatabaseDoesNotExistError,
    DirectoryDoesNotExistError,
    DirectoryIsAFileError,
    TableDoesNotExistError,
)
from pg2pyrquet.utils.postgres import (
    check_db_exists,
    check_table_exists,
    get_database_tables,
    get_databases_list,
    get_postgres_dsn,
    get_table_data_types,
    validate_database_exists,
    validate_table_exists,
)


@patch(
    "pg2pyrquet.utils.postgres.settings",
    postgres_user="user",
    postgres_password="pass",
    postgres_host="host",
    postgres_port="5432",
)
def test_get_postgres_dsn(mock_settings):
    dsn = get_postgres_dsn(database="test_db")
    expected_dsn = "postgresql://user:pass@host:5432/test_db"
    assert dsn == expected_dsn


@patch("pg2pyrquet.utils.postgres.adbc_connect")
def test_get_table_data_types(mock_adbc_connect):
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("field1", pa.int32()),
        ("field2", pa.string()),
    ]
    mock_adbc_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )
    result = get_table_data_types(dsn="dsn", table="test_table")
    expected = {"field1": pa.int32(), "field2": pa.string()}
    assert result == expected


@patch("pg2pyrquet.utils.postgres.psycopg2.connect")
def test_get_databases_list(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("db1",), ("db2",)]
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    databases = get_databases_list()
    assert databases == ["db1", "db2"]


@patch(
    "pg2pyrquet.utils.postgres.get_databases_list", return_value=["test_db"]
)
def test_check_db_exists(mock_get_databases_list):
    assert check_db_exists(database="test_db") is True


@patch(
    "pg2pyrquet.utils.postgres.get_databases_list", return_value=["other_db"]
)
def test_check_db_does_not_exist(mock_get_databases_list):
    assert check_db_exists(database="test_db") is False


@patch("pg2pyrquet.utils.postgres.psycopg2.connect")
def test_get_database_tables(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )
    tables = get_database_tables(database="test_db")
    assert tables == ["table1", "table2"]


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["test_table"],
)
def test_check_table_exists(mock_get_database_tables):
    assert check_table_exists(database="test_db", table="test_table") is True


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["other_table"],
)
def test_check_table_does_not_exist(mock_get_database_tables):
    assert check_table_exists(database="test_db", table="test_table") is False


def test_directory_does_not_exist_error():
    with pytest.raises(DirectoryDoesNotExistError):
        raise DirectoryDoesNotExistError("Test error")


def test_directory_is_a_file_error():
    with pytest.raises(DirectoryIsAFileError):
        raise DirectoryIsAFileError("Test error")


def test_database_does_not_exist_error():
    with pytest.raises(DatabaseDoesNotExistError):
        raise DatabaseDoesNotExistError("Test error")


def test_table_does_not_exist_error():
    with pytest.raises(TableDoesNotExistError):
        raise TableDoesNotExistError("Test error")


@patch("pg2pyrquet.utils.postgres.check_db_exists", return_value=True)
def test_validate_database_exists(mock_check_db_exists):
    validate_database_exists(database="test_db")


@patch("pg2pyrquet.utils.postgres.check_db_exists", return_value=False)
def test_validate_database_does_not_exist(mock_check_db_exists):
    with pytest.raises(DatabaseDoesNotExistError):
        validate_database_exists(database="test_db")


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=True)
def test_validate_table_exists(mock_check_table_exists):
    validate_table_exists(database="test_db", table="test_table")


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=False)
def test_validate_table_does_not_exist(mock_check_table_exists):
    with pytest.raises(TableDoesNotExistError):
        validate_table_exists(database="test_db", table="test_table")
