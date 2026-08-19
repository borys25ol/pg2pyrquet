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
    TableDoesNotExistError,
)
from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)

# Query to select all rows from a specified table
SELECT_ALL_TABLE_QUERY = "SELECT * FROM {table_name};"

# Query to list all tables in the 'public' schema of the current database
SELECT_TABLES_QUERY = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
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


def get_postgres_dsn(host: str, port: str, database: str) -> str:
    """
    Generates the PostgreSQL DSN (Data Source Name) for the specified database.

    Args:
        host (str): The hostname of the PostgreSQL server.
        port (str): The port number of the PostgreSQL server.
        database (str): The name of the PostgreSQL database.

    Returns:
        str: The connection string for the PostgreSQL database.
    """
    auth = get_postgres_auth()
    return f"postgresql://{auth}@{host}:{port}/{database}"


def get_default_query(table: str) -> str:
    """
    Generates the default query to select all rows from the specified table.

    Args:
        table (str): The name of the table to query.

    Returns:
        str: The default query to select all rows from the table.
    """
    query = sql.SQL(SELECT_ALL_TABLE_QUERY).format(
        table_name=sql.Identifier(table)
    )
    return query.as_string()


def check_db_exists(dsn: str) -> bool:
    """
    Checks if a database with the specified name exists.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    try:
        with psycopg.connect(dsn):
            return True
    except psycopg.OperationalError as e:
        logger.error(f"Error connecting to database: {e}")
        return False


def get_database_tables(dsn: str) -> list[str]:
    """
    Retrieves the list of all tables in the specified database.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.

    Returns:
        list[str]: A list of table names.
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_TABLES_QUERY)
            return [table_name for (table_name,) in cur.fetchall()]


def check_table_exists(dsn: str, table: str) -> bool:
    """
    Checks if a table with the specified name exists in the given database.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        table (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    return table in get_database_tables(dsn=dsn)


def validate_database_connection(dsn: str) -> str:
    """
    Validates that the specified database exists.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.

    Returns:
        str: The validated database name.

    Raises:
        DatabaseConnectionError: If the database does not exist.
    """
    parsed = urlparse(dsn)

    if not check_db_exists(dsn=dsn):
        raise DatabaseConnectionError(
            f"Database does not exist: {parsed.path}"
        )
    return dsn


def validate_table_exists(dsn: str, table: str) -> str:
    """
    Validates that the specified table exists within the given database.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        table (str): The name of the table to check.

    Returns:
        str: The validated table name.

    Raises:
        TableDoesNotExistError: If the table does not exist in the specified database.
    """
    if not check_table_exists(dsn=dsn, table=table):
        raise TableDoesNotExistError(
            f"Table '{table}' does not exist in database."
        )
    return table
