"""
Parquet export.

This module owns the data path and runs on ADBC, which returns Arrow
batches directly, so no value is converted to a Python object.

Every export runs in a read-only transaction, so the server refuses any
statement that would write, including a DELETE hidden inside a CTE.

The metadata path lives in `pg2pyrquet.utils.postgres` and runs on
psycopg.
"""

from pathlib import Path

from adbc_driver_postgresql import StatementOptions
from adbc_driver_postgresql.dbapi import connect
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.exceptions import InvalidQueryError
from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)

# Makes the server reject any statement that would write
READ_ONLY_TRANSACTION = "SET TRANSACTION READ ONLY"

# SQLSTATE raised when a statement tries to write in a read-only
# transaction. Postgres reports it against the outer SELECT, so the
# message needs translating before a user sees it.
READ_ONLY_SQLSTATE = "25006"


def normalise_query(query: str) -> str:
    """
    Strips whitespace and any trailing semicolon from the query.

    The driver wraps the query in `COPY (...) TO STDOUT`, so a trailing
    semicolon becomes a syntax error inside the parentheses. SQL files
    almost always end with one.

    Args:
        query (str): The query to normalise.

    Returns:
        str: The query without a trailing semicolon.
    """
    return query.strip().rstrip(";").strip()


def export_to_parquet(
    dsn: str,
    output_file: Path,
    query: str,
    batch_size_bytes: int,
    row_group_size: int,
) -> None:
    """
    Streams the query result into a Parquet file.

    The driver returns Arrow batches directly, so no value is turned into
    a Python object on the way. The reader carries the schema, so the
    query runs once.

    Args:
        dsn (str): The Data Source Name for the PostgreSQL database.
        output_file (Path): The path to the output Parquet file.
        query (str): SQL query to execute.
        batch_size_bytes (int): How much the driver reads per batch.
        row_group_size (int): Maximum rows per Parquet row group.
    """
    with connect(uri=dsn) as conn, conn.cursor() as cur:
        cur.adbc_statement.set_options(
            **{
                StatementOptions.BATCH_SIZE_HINT_BYTES.value: str(
                    batch_size_bytes
                )
            }
        )
        logger.info("Connected to DB, starting to execute query...")
        cur.execute(READ_ONLY_TRANSACTION)

        try:
            cur.execute(normalise_query(query=query))
        except Exception as error:
            if READ_ONLY_SQLSTATE not in str(error):
                raise

            raise InvalidQueryError(
                "The query would modify the database, and pg2pyrquet"
                " only reads. Every export runs in a read-only"
                " transaction, which also covers a write hidden inside"
                " a CTE."
            ) from error
        logger.info("Query executed...")

        reader = cur.fetch_record_batch()

        with ParquetWriter(where=output_file, schema=reader.schema) as writer:
            for number, batch in enumerate(reader, start=1):
                logger.info(
                    f"Writing batch {number} to the file: {output_file}"
                )
                writer.write_batch(batch=batch, row_group_size=row_group_size)

    logger.info("Export finished successfully.")
