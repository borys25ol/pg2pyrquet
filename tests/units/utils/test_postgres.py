import os
from unittest.mock import MagicMock, patch

import pytest
from psycopg import OperationalError

from pg2pyrquet.core.exceptions import (
    DatabaseConnectionError,
    InvalidPostgresCredentialsError,
    MissingDatabaseError,
    TableDoesNotExistError,
)
from pg2pyrquet.utils.postgres import (
    SELECT_TABLES_QUERY,
    build_dsn,
    check_table_exists,
    get_database_tables,
    get_default_query,
    get_postgres_auth,
    get_postgres_dsn,
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


def test_get_default_query_valid_table():
    table = "test_table"
    expected = 'SELECT * FROM "public"."test_table";'
    assert get_default_query(table=table) == expected


def test_get_default_query_quotes_mixed_case_table():
    table = "MyTable"
    expected = 'SELECT * FROM "public"."MyTable";'
    assert get_default_query(table=table) == expected


def test_get_default_query_escapes_embedded_quote():
    table = 'evil"; DROP TABLE users; --'
    expected = 'SELECT * FROM "public"."evil""; DROP TABLE users; --";'
    assert get_default_query(table=table) == expected


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_get_database_tables_with_tables(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
    connection = mock_connect.return_value.__enter__.return_value
    connection.cursor.return_value.__enter__.return_value = mock_cursor

    dsn = "postgresql://user:password@localhost:5432/testdb"
    result = get_database_tables(dsn)
    expected = ["table1", "table2"]
    assert result == expected
    mock_cursor.execute.assert_called_once_with(
        SELECT_TABLES_QUERY, ("public",)
    )


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_get_database_tables_without_tables(mock_connect):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    connection = mock_connect.return_value.__enter__.return_value
    connection.cursor.return_value.__enter__.return_value = mock_cursor

    dsn = "postgresql://user:password@localhost:5432/testdb"
    result = get_database_tables(dsn)
    expected = []
    assert result == expected
    mock_cursor.execute.assert_called_once_with(
        SELECT_TABLES_QUERY, ("public",)
    )


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["test_table"],
)
def test_check_table_exists(mock_get_database_tables):
    dsn = "test_dsn"
    table = "test_table"
    assert check_table_exists(dsn, table) is True
    mock_get_database_tables.assert_called_once_with(dsn=dsn, schema="public")


@patch(
    "pg2pyrquet.utils.postgres.get_database_tables",
    return_value=["other_table"],
)
def test_check_table_does_not_exist(mock_get_database_tables):
    dsn = "test_dsn"
    table = "test_table"
    assert check_table_exists(dsn, table) is False
    mock_get_database_tables.assert_called_once_with(dsn=dsn, schema="public")


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=True)
def test_validate_table_exists(mock_check_table_exists):
    dsn = "test_dsn"
    table = "test_table"
    assert validate_table_exists(dsn, table) == table
    mock_check_table_exists.assert_called_once_with(
        dsn=dsn, table=table, schema="public"
    )


@patch("pg2pyrquet.utils.postgres.check_table_exists", return_value=False)
def test_validate_table_does_not_exist(mock_check_table_exists):
    dsn = "test_dsn"
    table = "test_table"
    with pytest.raises(TableDoesNotExistError):
        validate_table_exists(dsn, table)
    mock_check_table_exists.assert_called_once_with(
        dsn=dsn, table=table, schema="public"
    )


@patch.dict(
    os.environ,
    {"POSTGRES_USER": "user@corp", "POSTGRES_PASSWORD": "p@ss:w/rd#1"},
)
def test_get_postgres_auth_escapes_special_characters():
    assert get_postgres_auth() == "user%40corp:p%40ss%3Aw%2Frd%231"


@patch(
    "pg2pyrquet.utils.postgres.psycopg.connect",
    side_effect=OperationalError("fe_sendauth: no password supplied"),
)
def test_validate_connection_reports_the_real_cause(mock_connect):
    dsn = "postgresql://user@localhost:5432/testdb"

    with pytest.raises(DatabaseConnectionError) as error:
        validate_database_connection(dsn=dsn)

    assert "no password supplied" in str(error.value)


@patch("pg2pyrquet.utils.postgres.psycopg.connect")
def test_validate_connection_returns_the_dsn(mock_connect):
    dsn = "postgresql://user:password@localhost:5432/testdb"

    assert validate_database_connection(dsn=dsn) == dsn
    mock_connect.return_value.__exit__.assert_called_once()


def test_get_default_query_uses_the_given_schema():
    query = get_default_query(table="events", schema="analytics")

    assert query == 'SELECT * FROM "analytics"."events";'


def test_build_dsn_returns_an_explicit_dsn_untouched():
    dsn = "postgresql://user@host:6432/db?sslmode=require"

    result = build_dsn(dsn=dsn, host="ignored", port=1, database="ignored")

    assert result == dsn


@patch("pg2pyrquet.utils.postgres.get_postgres_auth", return_value="")
def test_build_dsn_assembles_the_parts(mock_get_postgres_auth):
    result = build_dsn(
        dsn=None, host="localhost", port=5432, database="testdb"
    )

    assert result == "postgresql://@localhost:5432/testdb"


def test_build_dsn_requires_a_database_without_a_dsn():
    with pytest.raises(MissingDatabaseError):
        build_dsn(dsn=None, host="localhost", port=5432, database=None)
