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
    validate_database_connection,
    validate_table_exists,
)

app = typer.Typer()
logger = get_logger(name=__name__)


DEFAULT_BATCH_SIZE_BYTES = 16 * 1024 * 1024
DEFAULT_ROW_GROUP_SIZE = 1_048_576


@app.command()
def export_database(
    host: Annotated[str, typer.Option("--host")],
    port: Annotated[str, typer.Option("--port")],
    database: Annotated[str, typer.Option("--database")],
    output_path: Annotated[str, typer.Option("--folder")],
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps every table of the database to Parquet files.

    Args:
        host (str): The host of the PostgreSQL database.
        port (str): The port of the PostgreSQL database.
        database (str): The name of the PostgreSQL database.
        output_path (str): The directory for the Parquet files.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = get_postgres_dsn(host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)

    tables = get_database_tables(dsn=dsn)
    logger.info(f"Found tables to dump: {tables}")

    output_path = validate_output_path(output_path=output_path)

    for table in tables:
        logger.info(f"Starting to dump table: {table}")
        query = get_default_query(table=table)
        export_to_parquet(
            dsn=dsn,
            output_file=output_path / f"{table}.parquet",
            query=query,
            batch_size_bytes=batch_size_bytes,
            row_group_size=row_group_size,
        )


@app.command()
def export_table(
    host: Annotated[str, typer.Option("--host")],
    port: Annotated[str, typer.Option("--port")],
    database: Annotated[str, typer.Option("--database")],
    table: Annotated[str, typer.Option("--table")],
    output_path: Annotated[str, typer.Option("--folder")],
    output_file: str = "output.parquet",
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps the specified table to a Parquet file.

    Args:
        host (str): The host of the PostgreSQL database.
        port (str): The port of the PostgreSQL database.
        database (str): The name of the PostgreSQL database.
        table (str): The name of the table to dump.
        output_path (str): The directory for the Parquet file.
        output_file (str, optional): The name of the output Parquet
            file.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = get_postgres_dsn(host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)

    table = validate_table_exists(dsn=dsn, table=table)
    output_path = validate_output_path(output_path=output_path)

    query = get_default_query(table=table)

    logger.info(f"Starting to dump table: {table}")
    export_to_parquet(
        dsn=dsn,
        output_file=output_path / output_file,
        query=query,
        batch_size_bytes=batch_size_bytes,
        row_group_size=row_group_size,
    )


@app.command()
def export_query(
    host: Annotated[str, typer.Option("--host")],
    port: Annotated[str, typer.Option("--port")],
    database: Annotated[str, typer.Option("--database")],
    query_file: Annotated[str, typer.Option("--query-file")],
    output_path: Annotated[str, typer.Option("--folder")],
    output_file: str = "custom-query.parquet",
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps the result of a custom query to a Parquet file.

    Args:
        host (str): The host of the PostgreSQL database.
        port (str): The port of the PostgreSQL database.
        database (str): The name of the PostgreSQL database.
        query_file (str): The path of the file with SQL query.
        output_path (str): The directory for the Parquet file.
        output_file (str, optional): The name of the output Parquet
            file.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = get_postgres_dsn(host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)
    output_path = validate_output_path(output_path=output_path)
    query_path = validate_query_path(query_path=query_file)

    query = read_query_from_file(query_path=query_path)

    logger.info(f"Starting to dump custom query: {query}")
    export_to_parquet(
        dsn=dsn,
        output_file=output_path / output_file,
        query=query,
        batch_size_bytes=batch_size_bytes,
        row_group_size=row_group_size,
    )


if __name__ == "__main__":
    app()
