from typing import Annotated

import typer

from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.utils.parquet import process_export_to_parquet
from pg2pyrquet.utils.path import validate_output_path
from pg2pyrquet.utils.postgres import (
    get_database_tables,
    get_postgres_dsn,
    get_table_data_types,
    validate_database_exists,
    validate_table_exists,
)

app = typer.Typer()
logger = get_logger(name=__file__)


DEFAULT_BATCH_SIZE = 10000


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

        process_export_to_parquet(
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

    process_export_to_parquet(
        dsn=dsn,
        table=table,
        output_file=output_path / output_file,
        batch_size=batch_size,
        data_types=fields_types,
    )


if __name__ == "__main__":
    app()
