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
    format_query_with_limit,
    get_database_tables,
    get_databases_list,
    get_default_query,
    get_postgres_dsn,
    get_query_data_types,
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


def test_get_default_query_valid_table():
    table = "test_table"
    expected = "SELECT * FROM test_table;"
    assert get_default_query(table=table) == expected


def test_get_default_query_empty_table():
    table = ""
    expected = "SELECT * FROM ;"
    assert get_default_query(table=table) == expected


def test_get_default_query_special_characters():
    table = "test_table$123"
    expected = "SELECT * FROM test_table$123;"
    assert get_default_query(table=table) == expected


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.format_query_with_limit",
    return_value="SELECT * FROM test_table LIMIT 1;",
)
def test_get_query_data_types(
    mock_format_query_with_limit, mock_adbc_connect
):
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("field1", pa.int32()),
        ("field2", pa.string()),
    ]
    mock_adbc_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "test_dsn"
    query = "SELECT * FROM test_table"
    result = get_query_data_types(dsn, query)
    expected = {"field1": pa.int32(), "field2": pa.string()}
    assert result == expected
    mock_format_query_with_limit.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM test_table LIMIT 1;"
    )


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.format_query_with_limit",
    return_value=" LIMIT 1;",
)
def test_get_query_data_types_empty_query(
    mock_format_query_with_limit, mock_adbc_connect
):
    mock_cursor = MagicMock()
    mock_cursor.description = []
    mock_adbc_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "test_dsn"
    query = ""
    result = get_query_data_types(dsn, query)
    expected = {}
    assert result == expected
    mock_format_query_with_limit.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(" LIMIT 1;")


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.format_query_with_limit",
    return_value="SELECT * FROM test_table LIMIT 1;",
)
def test_get_query_data_types_with_limit(
    mock_format_query_with_limit, mock_adbc_connect
):
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("field1", pa.int32()),
        ("field2", pa.string()),
    ]
    mock_adbc_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "test_dsn"
    query = "SELECT * FROM test_table LIMIT 10"
    result = get_query_data_types(dsn, query)
    expected = {"field1": pa.int32(), "field2": pa.string()}
    assert result == expected
    mock_format_query_with_limit.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM test_table LIMIT 1;"
    )


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
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


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
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


def test_format_query_without_limit():
    query = "SELECT * FROM test_table"
    expected = "SELECT * FROM test_table LIMIT 1;"
    assert format_query_with_limit(query=query) == expected


def test_format_query_with_limit():
    query = "SELECT * FROM test_table LIMIT 10"
    expected = "SELECT * FROM test_table LIMIT 1;"
    assert format_query_with_limit(query=query) == expected


def test_format_query_with_semicolon():
    query = "SELECT * FROM test_table;"
    expected = "SELECT * FROM test_table LIMIT 1;"
    assert format_query_with_limit(query=query) == expected


def test_format_query_case_insensitivity():
    query = "SELECT * FROM test_table limit 5"
    expected = "SELECT * FROM test_table LIMIT 1;"
    assert format_query_with_limit(query=query) == expected
