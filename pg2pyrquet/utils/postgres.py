import psycopg2
from adbc_driver_postgresql.dbapi import connect as adbc_connect
from pyarrow import DataType

from pg2pyrquet.core.config import get_app_settings
from pg2pyrquet.core.exceptions import (
    DatabaseDoesNotExistError,
    TableDoesNotExistError,
)

settings = get_app_settings()

# Query to select the first row from a specified table
SELECT_FIRST_ROW_QUERY = "SELECT * FROM {table_name} LIMIT 1;"

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


def get_postgres_dsn(database: str) -> str:
    """
    Generates the PostgreSQL DSN (Data Source Name) for the specified database.

    Args:
        database (str): The name of the PostgreSQL database.

    Returns:
        str: The connection string for the PostgreSQL database.
    """
    return (
        f"postgresql://{settings.postgres_user}:{settings.postgres_password}@"
        f"{settings.postgres_host}:{settings.postgres_port}/{database}"
    )


def get_table_data_types(dsn: str, table: str) -> dict[str, DataType]:
    """
    Retrieves the data types of columns in the specified table.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        table (str): The name of the table.

    Returns:
        dict[str, DataType]: A dictionary mapping column names to their data types.
    """
    with adbc_connect(uri=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_FIRST_ROW_QUERY.format(table_name=table))
            return {column[0]: column[1] for column in cur.description}


def get_databases_list() -> list[str]:
    """
    Retrieves the list of all databases in the PostgreSQL instance.

    Returns:
        list[str]: A list of database names.
    """
    dsn = get_postgres_dsn(database="postgres")

    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_DATABASES_QUERY)
            return [db_name for (db_name,) in cur.fetchall()]


def check_db_exists(database: str) -> bool:
    """
    Checks if a database with the specified name exists.

    Args:
        database (str): The name of the database to check.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    return database in get_databases_list()


def get_database_tables(database: str) -> list[str]:
    """
    Retrieves the list of all tables in the specified database.

    Args:
        database (str): The name of the PostgreSQL database.

    Returns:
        list[str]: A list of table names.
    """
    dsn = get_postgres_dsn(database=database)

    with psycopg2.connect(dsn=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_TABLES_QUERY)
            return [table_name for (table_name,) in cur.fetchall()]


def check_table_exists(database: str, table: str) -> bool:
    """
    Checks if a table with the specified name exists in the given database.

    Args:
        database (str): The name of the PostgreSQL database.
        table (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    return table in get_database_tables(database=database)


def validate_database_exists(database: str) -> str:
    """
    Validates that the specified database exists.

    Args:
        database (str): The name of the database to check.

    Returns:
        str: The validated database name.

    Raises:
        DatabaseDoesNotExistError: If the database does not exist.
    """
    if not check_db_exists(database=database):
        raise DatabaseDoesNotExistError(
            f"Database '{database}' does not exist."
        )
    return database


def validate_table_exists(database: str, table: str) -> str:
    """
    Validates that the specified table exists within the given database.

    Args:
        database (str): The name of the database to check.
        table (str): The name of the table to check.

    Returns:
        str: The validated table name.

    Raises:
        TableDoesNotExistError: If the table does not exist in the specified database.
    """
    if not check_table_exists(database=database, table=table):
        raise TableDoesNotExistError(
            f"Table '{table}' does not exist in database '{database}'."
        )
    return table
