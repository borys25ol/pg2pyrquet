import os
import re
from urllib.parse import urlparse

import psycopg
from adbc_driver_postgresql.dbapi import connect as adbc_connect
from pyarrow import DataType

from pg2pyrquet.core.exceptions import (
    DatabaseConnectionError,
    InvalidPostgresCredentialsError,
    TableDoesNotExistError,
)
from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)

# Query to select all rows from a specified table
SELECT_ALL_TABLE_QUERY = "SELECT * FROM {table_name};"

# Query to list all databases in the PostgreSQL instance
SELECT_DATABASES_QUERY = "SELECT datname FROM pg_database;"

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

    if user:
        if password:
            return f"{user}:{password}"
        raise InvalidPostgresCredentialsError(
            "POSTGRES_PASSWORD environment variable is not set."
        )
    return ""


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
    return SELECT_ALL_TABLE_QUERY.format(table_name=table)


def get_query_data_types(dsn: str, query: str) -> dict[str, DataType]:
    """
    Retrieves the data types of columns in the specified table.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        query (str): The query to execute to retrieve the data types.

    Returns:
        dict[str, DataType]: A dictionary mapping column names to their data types.
    """
    query_with_limit = format_query_with_limit(query=query)
    with adbc_connect(uri=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query_with_limit)
            return {column[0]: column[1] for column in cur.description}


def check_db_exists(dsn: str) -> bool:
    """
    Checks if a database with the specified name exists.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    try:
        psycopg.connect(dsn)
    except psycopg.OperationalError as e:
        logger.error(f"Error connecting to database: {e}")
        return False
    return True


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


def format_query_with_limit(query: str) -> str:
    """
    Format the specific query.

    Add a LIMIT 1 clause if the query does not already contain a LIMIT clause.

    Change the LIMIT clause to LIMIT 1 if the query already contains a LIMIT clause.

    Args:
        query (str): The query to format.

    Returns:
        str: The formatted query with LIMIT 1 clause.
    """
    query = query.replace(";", "")

    if "limit" not in query.lower():
        return f"{query} LIMIT 1;"

    return re.sub(r"(?i)limit\s+\d+", "LIMIT 1;", string=query)
