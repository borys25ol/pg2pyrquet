from pathlib import Path

from adbc_driver_postgresql import StatementOptions
from adbc_driver_postgresql.dbapi import connect
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)


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
    with connect(uri=dsn) as conn:
        with conn.cursor() as cur:
            cur.adbc_statement.set_options(
                **{
                    StatementOptions.BATCH_SIZE_HINT_BYTES.value: str(
                        batch_size_bytes
                    )
                }
            )
            logger.info("Connected to DB, starting to execute query...")
            cur.execute(query)
            logger.info("Query executed...")

            reader = cur.fetch_record_batch()

            with ParquetWriter(
                where=output_file, schema=reader.schema
            ) as writer:
                for number, batch in enumerate(reader, start=1):
                    logger.info(
                        f"Writing batch {number} to the file: {output_file}"
                    )
                    writer.write_batch(
                        batch=batch, row_group_size=row_group_size
                    )

    logger.info("Export finished successfully.")
