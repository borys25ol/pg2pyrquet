from typing import Annotated

import typer

from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.export import export_to_parquet
from pg2pyrquet.utils.files import read_query_from_file
from pg2pyrquet.utils.path import validate_output_path, validate_query_path
from pg2pyrquet.utils.postgres import (
    get_database_tables,
    get_default_query,
    get_postgres_dsn,
    validate_database_exists,
    validate_table_exists,
)

app = typer.Typer()
logger = get_logger(name=__name__)


DEFAULT_BATCH_SIZE = 10000


@app.command()
def export_database(
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
        query = get_default_query(table=table)
        export_to_parquet(
            dsn=dsn,
            output_file=output_path / f"{table}.parquet",
            batch_size=batch_size,
            query=query,
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

    query = get_default_query(table=table)

    logger.info(f"Starting to dump table: {table}")
    export_to_parquet(
        dsn=dsn,
        output_file=output_path / output_file,
        batch_size=batch_size,
        query=query,
    )


@app.command()
def export_query(
    database: Annotated[str, typer.Option("--database")],
    query_file: Annotated[str, typer.Option("--query-file")],
    output_path: Annotated[str, typer.Option("--folder")],
    output_file: str = "custom-query.parquet",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    Dumps the specified custom query from the given PostgreSQL database to a Parquet file.

    Args:
        database (str): The name of the PostgreSQL database.
        query_file (str): The path of the file with SQL query.
        output_path (str): The directory where the Parquet file will be saved.
        output_file (str, optional): The name of the output Parquet file. Defaults to "output.parquet".
        batch_size (int, optional): The number of rows to process in each batch. Defaults to DEFAULT_BATCH_SIZE.
    """
    dsn = get_postgres_dsn(database=database)

    validate_database_exists(database=database)
    output_path = validate_output_path(output_path=output_path)
    query_path = validate_query_path(query_path=query_file)

    query = read_query_from_file(query_path=query_path)

    logger.info(f"Starting to dump custom query: {query}")
    export_to_parquet(
        dsn=dsn,
        output_file=output_path / output_file,
        batch_size=batch_size,
        query=query,
    )


if __name__ == "__main__":
    app()
