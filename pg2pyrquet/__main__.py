from importlib.metadata import version
from typing import Annotated

import typer

from pg2pyrquet.core.errors import handle_cli_errors
from pg2pyrquet.core.exceptions import ExportFailedError
from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.export import export_to_parquet
from pg2pyrquet.utils.files import read_query_from_file
from pg2pyrquet.utils.path import (
    validate_output_file,
    validate_output_path,
    validate_query_path,
)
from pg2pyrquet.utils.postgres import (
    DEFAULT_SCHEMA,
    build_dsn,
    get_database_tables,
    get_default_query,
    validate_database_connection,
    validate_table_exists,
)

app = typer.Typer()
logger = get_logger(name=__name__)


def show_version(value: bool) -> None:
    """
    Prints the installed version and exits.

    Args:
        value (bool): Whether the flag was passed.
    """
    if value:
        typer.echo(version("pg2pyrquet"))
        raise typer.Exit()


@app.callback()
def main(
    version_flag: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=show_version,
            is_eager=True,
            help="Show the installed version and exit.",
        ),
    ] = False,
) -> None:
    """
    Exports PostgreSQL tables and queries to Parquet files.
    """


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_BATCH_SIZE_BYTES = 16 * 1024 * 1024
DEFAULT_ROW_GROUP_SIZE = 1_048_576


@app.command()
@handle_cli_errors
def export_database(
    output_path: Annotated[str, typer.Option("--folder")],
    host: Annotated[str, typer.Option("--host")] = DEFAULT_HOST,
    port: Annotated[int, typer.Option("--port")] = DEFAULT_PORT,
    database: Annotated[str | None, typer.Option("--database")] = None,
    dsn: Annotated[str | None, typer.Option("--dsn")] = None,
    schema: Annotated[str, typer.Option("--schema")] = DEFAULT_SCHEMA,
    overwrite: Annotated[bool, typer.Option("--overwrite")] = False,
    continue_on_error: Annotated[
        bool, typer.Option("--continue-on-error")
    ] = False,
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps every table of the database to Parquet files.

    Args:
        host (str): The host of the PostgreSQL database.
        port (int): The port of the PostgreSQL database.
        database (str | None): The name of the PostgreSQL database.
        dsn (str | None): A complete DSN, used instead of the parts.
        output_path (str): The directory for the Parquet files.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = build_dsn(dsn=dsn, host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)

    tables = get_database_tables(dsn=dsn, schema=schema)
    logger.info(f"Found tables to dump: {tables}")

    output_path = validate_output_path(output_path=output_path)
    failed: list[str] = []

    for table in tables:
        logger.info(f"Starting to dump table: {table}")

        try:
            output_file = validate_output_file(
                output_file=output_path / f"{table}.parquet",
                overwrite=overwrite,
            )
            export_to_parquet(
                dsn=dsn,
                output_file=output_file,
                query=get_default_query(table=table, schema=schema),
                batch_size_bytes=batch_size_bytes,
                row_group_size=row_group_size,
            )
        except Exception as error:
            if not continue_on_error:
                raise

            logger.error(f"Failed to dump table {table}: {error}")
            failed.append(table)

    logger.info(
        f"Exported {len(tables) - len(failed)} of {len(tables)} tables."
    )

    if failed:
        raise ExportFailedError(
            f"These tables failed to export: {', '.join(failed)}"
        )


@app.command()
@handle_cli_errors
def export_table(
    table: Annotated[str, typer.Option("--table")],
    output_path: Annotated[str, typer.Option("--folder")],
    host: Annotated[str, typer.Option("--host")] = DEFAULT_HOST,
    port: Annotated[int, typer.Option("--port")] = DEFAULT_PORT,
    database: Annotated[str | None, typer.Option("--database")] = None,
    dsn: Annotated[str | None, typer.Option("--dsn")] = None,
    schema: Annotated[str, typer.Option("--schema")] = DEFAULT_SCHEMA,
    overwrite: Annotated[bool, typer.Option("--overwrite")] = False,
    output_file: str = "output.parquet",
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps the specified table to a Parquet file.

    Args:
        host (str): The host of the PostgreSQL database.
        port (int): The port of the PostgreSQL database.
        database (str | None): The name of the PostgreSQL database.
        dsn (str | None): A complete DSN, used instead of the parts.
        table (str): The name of the table to dump.
        output_path (str): The directory for the Parquet file.
        output_file (str, optional): The name of the output Parquet
            file.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = build_dsn(dsn=dsn, host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)

    table = validate_table_exists(dsn=dsn, table=table, schema=schema)
    output_path = validate_output_path(output_path=output_path)
    target_file = validate_output_file(
        output_file=output_path / output_file, overwrite=overwrite
    )

    query = get_default_query(table=table, schema=schema)

    logger.info(f"Starting to dump table: {table}")
    export_to_parquet(
        dsn=dsn,
        output_file=target_file,
        query=query,
        batch_size_bytes=batch_size_bytes,
        row_group_size=row_group_size,
    )


@app.command()
@handle_cli_errors
def export_query(
    query_file: Annotated[str, typer.Option("--query-file")],
    output_path: Annotated[str, typer.Option("--folder")],
    host: Annotated[str, typer.Option("--host")] = DEFAULT_HOST,
    port: Annotated[int, typer.Option("--port")] = DEFAULT_PORT,
    database: Annotated[str | None, typer.Option("--database")] = None,
    dsn: Annotated[str | None, typer.Option("--dsn")] = None,
    overwrite: Annotated[bool, typer.Option("--overwrite")] = False,
    output_file: str = "custom-query.parquet",
    batch_size_bytes: int = DEFAULT_BATCH_SIZE_BYTES,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
) -> None:
    """
    Dumps the result of a custom query to a Parquet file.

    Args:
        host (str): The host of the PostgreSQL database.
        port (int): The port of the PostgreSQL database.
        database (str | None): The name of the PostgreSQL database.
        dsn (str | None): A complete DSN, used instead of the parts.
        query_file (str): The path of the file with SQL query.
        output_path (str): The directory for the Parquet file.
        output_file (str, optional): The name of the output Parquet
            file.
        batch_size_bytes (int, optional): How much the driver reads
            per batch, in bytes.
        row_group_size (int, optional): Maximum rows per Parquet row
            group.
    """
    dsn = build_dsn(dsn=dsn, host=host, port=port, database=database)

    validate_database_connection(dsn=dsn)
    output_path = validate_output_path(output_path=output_path)
    target_file = validate_output_file(
        output_file=output_path / output_file, overwrite=overwrite
    )
    query_path = validate_query_path(query_path=query_file)

    query = read_query_from_file(query_path=query_path)

    logger.info(f"Starting to dump custom query: {query}")
    export_to_parquet(
        dsn=dsn,
        output_file=target_file,
        query=query,
        batch_size_bytes=batch_size_bytes,
        row_group_size=row_group_size,
    )


if __name__ == "__main__":
    app()
