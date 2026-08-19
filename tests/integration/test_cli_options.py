from urllib.parse import urlparse

import psycopg
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from pg2pyrquet.__main__ import app
from pg2pyrquet.utils.postgres import get_database_tables

pytestmark = pytest.mark.integration

runner = CliRunner()


@pytest.fixture
def connection_args(postgres_dsn, monkeypatch):
    """
    Returns CLI connection arguments for the test database.
    """
    parsed = urlparse(postgres_dsn)
    monkeypatch.setenv("POSTGRES_USER", parsed.username or "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password or "postgres")

    return [
        "--host",
        parsed.hostname,
        "--port",
        str(parsed.port),
        "--database",
        parsed.path.lstrip("/"),
    ]


@pytest.fixture
def other_schema(postgres_dsn):
    """
    Creates a table in a non-public schema and drops it afterwards.
    """
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS analytics CASCADE;")
        conn.execute("CREATE SCHEMA analytics;")
        conn.execute("CREATE TABLE analytics.events (id int);")
        conn.execute("INSERT INTO analytics.events VALUES (42);")

    yield "analytics"

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS analytics CASCADE;")


def test_schema_option_reaches_another_schema(
    connection_args, other_schema, tmp_path
):
    result = runner.invoke(
        app,
        [
            "export-table",
            *connection_args,
            "--schema",
            other_schema,
            "--table",
            "events",
            "--folder",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert pq.read_table(tmp_path / "output.parquet").to_pylist() == [
        {"id": 42}
    ]


def test_existing_file_is_kept_without_overwrite(
    connection_args, seeded_table, tmp_path
):
    output_file = tmp_path / "output.parquet"
    output_file.write_text("not a parquet file")

    result = runner.invoke(
        app,
        [
            "export-table",
            *connection_args,
            "--table",
            seeded_table,
            "--folder",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert output_file.read_text() == "not a parquet file"


def test_overwrite_replaces_an_existing_file(
    connection_args, seeded_table, tmp_path
):
    output_file = tmp_path / "output.parquet"
    output_file.write_text("not a parquet file")

    result = runner.invoke(
        app,
        [
            "export-table",
            *connection_args,
            "--table",
            seeded_table,
            "--folder",
            str(tmp_path),
            "--overwrite",
        ],
    )

    assert result.exit_code == 0, result.output
    assert pq.read_table(output_file).num_rows == 2


def export_database(connection_args, folder, extra=()):
    """
    Runs export-database into the given folder.
    """
    return runner.invoke(
        app,
        [
            "export-database",
            *connection_args,
            "--folder",
            str(folder),
            *extra,
        ],
    )


@pytest.fixture
def second_table(postgres_dsn):
    """
    Adds a second table, so a partial export has something to succeed at.
    """
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS healthy;")
        conn.execute("CREATE TABLE healthy (id int);")
        conn.execute("INSERT INTO healthy VALUES (1);")

    yield "healthy"

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS healthy;")


def block_first_table(postgres_dsn, folder) -> list[str]:
    """
    Makes the first table of the export fail by blocking its output file.

    Returns the remaining table names, which a healthy run must export.
    """
    tables = get_database_tables(dsn=postgres_dsn)
    (folder / f"{tables[0]}.parquet").write_text("blocked")
    remaining = tables[1:]

    assert remaining, "the fixtures must leave more than one table"

    return remaining


def test_a_failing_table_stops_the_run_by_default(
    connection_args, postgres_dsn, seeded_table, second_table, tmp_path
):
    remaining = block_first_table(postgres_dsn, tmp_path)

    result = export_database(connection_args, tmp_path)

    assert result.exit_code == 1
    assert [
        name for name in remaining if (tmp_path / f"{name}.parquet").exists()
    ] == []


def test_continue_on_error_exports_the_healthy_tables(
    connection_args, postgres_dsn, seeded_table, second_table, tmp_path
):
    remaining = block_first_table(postgres_dsn, tmp_path)

    result = export_database(
        connection_args, tmp_path, extra=["--continue-on-error"]
    )

    assert result.exit_code == 1
    for name in remaining:
        assert (tmp_path / f"{name}.parquet").exists()
