from collections import defaultdict
from pathlib import Path
from typing import Annotated

import psycopg2
import pyarrow as pa
import typer
from psycopg2.extras import RealDictCursor
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.utils.parquet import write_batch_to_parquet
from pg2pyrquet.utils.path import validate_output_path
from pg2pyrquet.utils.postgres import (
    SELECT_ALL_TABLE_QUERY,
    get_database_tables,
    get_postgres_dsn,
    get_table_data_types,
    validate_database_exists,
    validate_table_exists,
)

app = typer.Typer()
logger = get_logger(name=__file__)


DEFAULT_BATCH_SIZE = 10000


def reset_column_values(
    fields_types: dict[str, pa.DataType], records: dict[str, list]
) -> None:
    """
    Resets the values list for each column in the records dictionary based on the fields_types.

    Args:
        fields_types (dict[str, pa.DataType]): The dictionary of field names and their data types.
        records (dict[str, list]): The dictionary to reset, where keys are field names.
    """
    for field in fields_types:
        records[field] = []


def _process_export_to_parquet(
    dsn: str,
    table: str,
    output_file: Path,
    batch_size: int,
    data_types: dict[str, pa.DataType],
) -> None:
    """
    Processes export the specified table from the database to a Parquet file.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        table (str): The name of the table to dump.
        output_file (Path): The path to the output Parquet file.
        batch_size (int): The number of rows to process in each batch.
        data_types (dict[str, pa.DataType]): A dictionary mapping column names to their data types.
    """
    records = defaultdict(list)
    schema = pa.schema(fields=data_types)

    with ParquetWriter(where=output_file, schema=schema) as writer:
        with psycopg2.connect(dsn=dsn) as conn:
            logger.info("Connected to DB, starting to execute query...")

            with conn.cursor(
                name="pg-to-parquet", cursor_factory=RealDictCursor
            ) as cur:
                cur.itersize = batch_size
                cur.execute(SELECT_ALL_TABLE_QUERY.format(table_name=table))
                logger.info("Query executed...")

                for index, record in enumerate(cur):
                    for column, value in record.items():
                        records[column].append(value)

                    if index % batch_size == 0:
                        logger.info(
                            f"Writing batch {index // batch_size + 1} to the file: {output_file}"
                        )
                        write_batch_to_parquet(
                            writer=writer,
                            fields_types=data_types,
                            data=records,
                            schema=schema,
                        )
                        reset_column_values(
                            fields_types=data_types, records=records
                        )

                write_batch_to_parquet(
                    writer=writer,
                    fields_types=data_types,
                    data=records,
                    schema=schema,
                )
                logger.info("Export finished successfully.")


@app.command()
def export_tables(
    database: Annotated[str, typer.Option("--database")],
    output_path: Annotated[str, typer.Option("--folder")],
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    Dumps all tables from the specified PostgreSQL database to Parquet files.

    Args:
        database (str): The name of the PostgreSQL database.
        output_path (str): The directory where Parquet files will be saved.
        batch_size (int, optional): The number of rows to process in each batch. Defaults to DEFAULT_BATCH_SIZE.
    """
    dsn = get_postgres_dsn(database=database)

    database = validate_database_exists(database=database)

    tables = get_database_tables(database=database)
    logger.info(f"Found tables to dump: {tables}")

    output_path = validate_output_path(output_path=output_path)

    for table in tables:
        logger.info(f"Starting to dump table: {table}")

        fields_types = get_table_data_types(dsn=dsn, table=table)

        _process_export_to_parquet(
            dsn=dsn,
            table=table,
            output_file=output_path / f"{table}.parquet",
            batch_size=batch_size,
            data_types=fields_types,
        )


@app.command()
def export_table(
    database: Annotated[str, typer.Option("--database")],
    table: Annotated[str, typer.Option("--table")],
    output_path: Annotated[str, typer.Option("--folder")],
    output_file: str = "output.parquet",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    Dumps the specified table from the given PostgreSQL database to a Parquet file.

    Args:
        database (str): The name of the PostgreSQL database.
        table (str): The name of the table to dump.
        output_path (str): The directory where the Parquet file will be saved.
        output_file (str, optional): The name of the output Parquet file. Defaults to "output.parquet".
        batch_size (int, optional): The number of rows to process in each batch. Defaults to DEFAULT_BATCH_SIZE.
    """
    dsn = get_postgres_dsn(database=database)

    database = validate_database_exists(database=database)
    table = validate_table_exists(database=database, table=table)
    output_path = validate_output_path(output_path=output_path)

    logger.info(f"Starting to dump table: {table}")

    fields_types = get_table_data_types(dsn=dsn, table=table)

    _process_export_to_parquet(
        dsn=dsn,
        table=table,
        output_file=output_path / output_file,
        batch_size=batch_size,
        data_types=fields_types,
    )


if __name__ == "__main__":
    app()
