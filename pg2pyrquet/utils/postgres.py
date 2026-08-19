"""
Postgres metadata access.

This module owns the metadata path: building a DSN, listing tables,
checking that a database answers, and quoting identifiers. It runs on
psycopg.

The data path lives in `pg2pyrquet.export` and runs on ADBC, which
returns Arrow batches directly.
"""

import os
from urllib.parse import quote_plus, urlparse

import psycopg
from psycopg import sql

from pg2pyrquet.core.exceptions import (
    DatabaseConnectionError,
    InvalidPostgresCredentialsError,
    MissingDatabaseError,
    TableDoesNotExistError,
)
from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)

# Default schema searched for tables
DEFAULT_SCHEMA = "public"

# Query to select all rows from a specified table
SELECT_ALL_TABLE_QUERY = "SELECT * FROM {schema}.{table_name};"

# Query to list all tables in the 'public' schema of the current database
SELECT_TABLES_QUERY = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s;
"""


def get_postgres_auth() -> str:
    """
    Retrieves the PostgreSQL user and password from environment variables.

    Returns:
        str: The authentication string for the PostgreSQL database.

    Raises:
        ValueError: If the POSTGRES_PASSWORD environment variable is not set.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    if not user:
        return ""

    if not password:
        raise InvalidPostgresCredentialsError(
            "POSTGRES_PASSWORD environment variable is not set."
        )

    return f"{quote_plus(user)}:{quote_plus(password)}"


def build_dsn(
    dsn: str | None, host: str, port: int, database: str | None
) -> str:
    """
    Returns the DSN to connect with.

    An explicit DSN wins and is used untouched, so libpq parameters such
    as sslmode keep working. Otherwise the parts are assembled, and the
    database name is required.

    Args:
        dsn (str | None): A complete DSN, or None to assemble one.
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number of the PostgreSQL server.
        database (str | None): The name of the PostgreSQL database.

    Returns:
        str: The DSN to connect with.

    Raises:
        MissingDatabaseError: If no DSN and no database name were given.
    """
    if dsn:
        return dsn

    if not database:
        raise MissingDatabaseError(
            "Pass --database, or pass --dsn with a complete connection"
            " string."
        )

    return get_postgres_dsn(host=host, port=port, database=database)


def get_postgres_dsn(host: str, port: int, database: str) -> str:
    """
    Generates the DSN (Data Source Name) for the given database.

    Args:
        host (str): The hostname of the PostgreSQL server.
        port (int): The port number of the PostgreSQL server.
        database (str): The name of the PostgreSQL database.

    Returns:
        str: The connection string for the database.
    """
    auth = get_postgres_auth()
    return f"postgresql://{auth}@{host}:{port}/{database}"


def get_default_query(table: str, schema: str = DEFAULT_SCHEMA) -> str:
    """
    Generates the query selecting every row of the given table.

    Args:
        table (str): The name of the table to query.
        schema (str): The schema holding the table.

    Returns:
        str: The query selecting every row of the table.
    """
    query = sql.SQL(SELECT_ALL_TABLE_QUERY).format(
        schema=sql.Identifier(schema), table_name=sql.Identifier(table)
    )
    return query.as_string()


def get_database_tables(dsn: str, schema: str = DEFAULT_SCHEMA) -> list[str]:
    """
    Retrieves the list of all tables in the specified database.

    Args:
        dsn (str): The Data Source Name for the PostgreSQL database.

    Returns:
        list[str]: A list of table names.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SELECT_TABLES_QUERY, (schema,))
        return [table_name for (table_name,) in cur.fetchall()]


def check_table_exists(
    dsn: str, table: str, schema: str = DEFAULT_SCHEMA
) -> bool:
    """
    Checks if a table with the specified name exists in the given database.

    Args:
        dsn (str): The Data Source Name for the PostgreSQL database.
        table (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    return table in get_database_tables(dsn=dsn, schema=schema)


def validate_database_connection(dsn: str) -> str:
    """
    Validates that the database answers, and reports why if it does not.

    Args:
        dsn (str): The Data Source Name for the PostgreSQL database.

    Returns:
        str: The validated DSN.

    Raises:
        DatabaseConnectionError: If the connection cannot be opened. The
            message carries the reason reported by the server, which is
            often authentication rather than a missing database.
    """
    parsed = urlparse(dsn)
    database = parsed.path.lstrip("/") or "<unnamed>"

    try:
        with psycopg.connect(dsn):
            return dsn
    except psycopg.OperationalError as error:
        raise DatabaseConnectionError(
            f"Cannot connect to database '{database}': {error}"
        ) from error


def validate_table_exists(
    dsn: str, table: str, schema: str = DEFAULT_SCHEMA
) -> str:
    """
    Validates that the specified table exists within the given database.

    Args:
        dsn (str): The Data Source Name for the PostgreSQL database.
        table (str): The name of the table to check.

    Returns:
        str: The validated table name.

    Raises:
        TableDoesNotExistError: If the table does not exist.
    """
    if not check_table_exists(dsn=dsn, table=table, schema=schema):
        raise TableDoesNotExistError(
            f"Table '{table}' does not exist in schema '{schema}'."
        )
    return table
