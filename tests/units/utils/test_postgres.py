import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from psycopg import OperationalError

from pg2pyrquet.core.exceptions import (
    DatabaseConnectionError,
    InvalidPostgresCredentialsError,
    TableDoesNotExistError,
)
from pg2pyrquet.utils.postgres import (
    SELECT_TABLES_QUERY,
    build_schema_probe_query,
    check_db_exists,
    check_table_exists,
    get_database_tables,
    get_default_query,
    get_postgres_auth,
    get_postgres_dsn,
    get_query_data_types,
    validate_database_connection,
    validate_table_exists,
)


@patch.dict(
    os.environ,
    {"POSTGRES_USER": "test_user", "POSTGRES_PASSWORD": "test_password"},
)
def test_get_postgres_auth_with_user_and_password():
    assert get_postgres_auth() == "test_user:test_password"


@patch.dict(os.environ, {"POSTGRES_USER": "test_user"})
def test_get_postgres_auth_with_user_only():
    with pytest.raises(InvalidPostgresCredentialsError):
        get_postgres_auth()


@patch.dict(os.environ, {}, clear=True)
def test_get_postgres_auth_without_user_and_password():
    assert get_postgres_auth() == ""


@patch(
    "pg2pyrquet.utils.postgres.get_postgres_auth",
    return_value="test_user:test_password",
)
def test_get_postgres_dsn(mock_get_postgres_auth):
    host = "localhost"
    port = "5432"
    database = "test_db"
    expected_dsn = (
        "postgresql://test_user:test_password@localhost:5432/test_db"
    )
    assert get_postgres_dsn(host, port, database) == expected_dsn
    mock_get_postgres_auth.assert_called_once()


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_check_db_exists(mock_connect):
    dsn = "postgresql://user:password@localhost:5432/testdb"
    assert check_db_exists(dsn) is True
    mock_connect.assert_called_once_with(dsn)


@patch(
    "pg2pyrquet.utils.postgres.psycopg.connect", side_effect=OperationalError
)
def test_check_db_does_not_exist(mock_connect):
    dsn = "postgresql://user:password@localhost:5432/testdb"
    assert check_db_exists(dsn) is False
    mock_connect.assert_called_once_with(dsn)


def test_get_default_query_valid_table():
    table = "test_table"
    expected = 'SELECT * FROM "test_table";'
    assert get_default_query(table=table) == expected


def test_get_default_query_quotes_mixed_case_table():
    table = "MyTable"
    expected = 'SELECT * FROM "MyTable";'
    assert get_default_query(table=table) == expected


def test_get_default_query_escapes_embedded_quote():
    table = 'evil"; DROP TABLE users; --'
    expected = 'SELECT * FROM "evil""; DROP TABLE users; --";'
    assert get_default_query(table=table) == expected


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.build_schema_probe_query",
    return_value="SELECT * FROM test_table LIMIT 1;",
)
def test_get_query_data_types(
    mock_build_schema_probe_query, mock_adbc_connect
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
    mock_build_schema_probe_query.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM test_table LIMIT 1;"
    )


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.build_schema_probe_query",
    return_value=" LIMIT 1;",
)
def test_get_query_data_types_empty_query(
    mock_build_schema_probe_query, mock_adbc_connect
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
    mock_build_schema_probe_query.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(" LIMIT 1;")


@patch("pg2pyrquet.utils.postgres.adbc_connect")
@patch(
    "pg2pyrquet.utils.postgres.build_schema_probe_query",
    return_value="SELECT * FROM test_table LIMIT 1;",
)
def test_get_query_data_types_with_limit(
    mock_build_schema_probe_query, mock_adbc_connect
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
    mock_build_schema_probe_query.assert_called_once_with(query=query)
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM test_table LIMIT 1;"
    )


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_get_database_tables_with_tables(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "postgresql://user:password@localhost:5432/testdb"
    result = get_database_tables(dsn)
    expected = ["table1", "table2"]
    assert result == expected
    mock_cursor.execute.assert_called_once_with(SELECT_TABLES_QUERY)


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_get_database_tables_without_tables(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "postgresql://user:password@localhost:5432/testdb"
    result = get_database_tables(dsn)
    expected = []
    assert result == expected
    mock_cursor.execute.assert_called_once_with(SELECT_TABLES_QUERY)


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["test_table"],
)
def test_check_table_exists(mock_get_database_tables):
    dsn = "test_dsn"
    table = "test_table"
    assert check_table_exists(dsn, table) is True
    mock_get_database_tables.assert_called_once_with(dsn=dsn)


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["other_table"],
)
def test_check_table_does_not_exist(mock_get_database_tables):
    dsn = "test_dsn"
    table = "test_table"
    assert check_table_exists(dsn, table) is False
    mock_get_database_tables.assert_called_once_with(dsn=dsn)


@patch("pg2pyrquet.utils.postgres.check_db_exists", return_value=True)
def test_validate_database_connection_exists(mock_check_db_exists):
    dsn = "postgresql://user:password@localhost:5432/testdb"
    assert validate_database_connection(dsn) == dsn
    mock_check_db_exists.assert_called_once_with(dsn=dsn)


@patch("pg2pyrquet.utils.postgres.check_db_exists", return_value=False)
def test_validate_database_connection_does_not_exist(mock_check_db_exists):
    dsn = "postgresql://user:password@localhost:5432/testdb"
    with pytest.raises(DatabaseConnectionError):
        validate_database_connection(dsn)
    mock_check_db_exists.assert_called_once_with(dsn=dsn)


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=True)
def test_validate_table_exists(mock_check_table_exists):
    dsn = "test_dsn"
    table = "test_table"
    assert validate_table_exists(dsn, table) == table
    mock_check_table_exists.assert_called_once_with(dsn=dsn, table=table)


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=False)
def test_validate_table_does_not_exist(mock_check_table_exists):
    dsn = "test_dsn"
    table = "test_table"
    with pytest.raises(TableDoesNotExistError):
        validate_table_exists(dsn, table)
    mock_check_table_exists.assert_called_once_with(dsn=dsn, table=table)


def test_build_schema_probe_query_wraps_query():
    query = "SELECT * FROM test_table"
    expected = (
        "SELECT * FROM (SELECT * FROM test_table) AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


def test_build_schema_probe_query_strips_trailing_semicolon():
    query = "SELECT * FROM test_table;"
    expected = (
        "SELECT * FROM (SELECT * FROM test_table) AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


def test_build_schema_probe_query_keeps_table_name_containing_limit():
    query = "SELECT * FROM delimiter_table"
    expected = (
        "SELECT * FROM (SELECT * FROM delimiter_table)"
        " AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


def test_build_schema_probe_query_keeps_string_literal_untouched():
    query = "SELECT * FROM test_table WHERE name = 'no limit here'"
    expected = (
        "SELECT * FROM (SELECT * FROM test_table"
        " WHERE name = 'no limit here') AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


def test_build_schema_probe_query_keeps_limit_offset_clause():
    query = "SELECT * FROM test_table ORDER BY id LIMIT 50 OFFSET 10;"
    expected = (
        "SELECT * FROM (SELECT * FROM test_table ORDER BY id"
        " LIMIT 50 OFFSET 10) AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


def test_build_schema_probe_query_keeps_nested_limit():
    query = "SELECT a, (SELECT b FROM c LIMIT 5) FROM test_table"
    expected = (
        "SELECT * FROM (SELECT a, (SELECT b FROM c LIMIT 5)"
        " FROM test_table) AS _schema_probe LIMIT 1;"
    )
    assert build_schema_probe_query(query=query) == expected


@patch.dict(
    os.environ,
    {"POSTGRES_USER": "user@corp", "POSTGRES_PASSWORD": "p@ss:w/rd#1"},
)
def test_get_postgres_auth_escapes_special_characters():
    assert get_postgres_auth() == "user%40corp:p%40ss%3Aw%2Frd%231"


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_check_db_exists_closes_connection(mock_connect):
    dsn = "postgresql://user:password@localhost:5432/testdb"
    check_db_exists(dsn=dsn)
    mock_connect.return_value.__exit__.assert_called_once()
