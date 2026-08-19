from urllib.parse import urlparse

import psycopg
import pytest
from typer.testing import CliRunner

from pg2pyrquet.__main__ import app
from pg2pyrquet.core.exceptions import InvalidQueryError
from pg2pyrquet.export import export_to_parquet

pytestmark = pytest.mark.integration

BATCH_SIZE_BYTES = 16 * 1024 * 1024
ROW_GROUP_SIZE = 1_048_576

# Passes a substring check for "select" while deleting every row
WRITING_QUERY = (
    "WITH deleted AS (DELETE FROM victim RETURNING *) SELECT * FROM deleted"
)


@pytest.fixture
def victim_table(postgres_dsn):
    """
    Creates a table a writing query would empty, and drops it afterwards.
    """
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS victim;")
        conn.execute("CREATE TABLE victim (id int);")
        conn.execute("INSERT INTO victim VALUES (1), (2), (3);")

    yield "victim"

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS victim;")


def count_rows(dsn: str) -> int:
    """
    Returns the number of rows left in the victim table.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute("SELECT count(*) FROM victim").fetchone()[0]


def test_a_writing_query_is_refused(postgres_dsn, victim_table, tmp_path):
    with pytest.raises(InvalidQueryError, match="only reads"):
        export_to_parquet(
            dsn=postgres_dsn,
            output_file=tmp_path / "refused.parquet",
            query=WRITING_QUERY,
            batch_size_bytes=BATCH_SIZE_BYTES,
            row_group_size=ROW_GROUP_SIZE,
        )

    assert count_rows(postgres_dsn) == 3


def test_a_writing_query_file_is_refused_by_the_cli(
    postgres_dsn, victim_table, tmp_path, monkeypatch, caplog
):
    parsed = urlparse(postgres_dsn)
    monkeypatch.setenv("POSTGRES_USER", parsed.username or "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password or "postgres")

    query_file = tmp_path / "writing.sql"
    query_file.write_text(f"{WRITING_QUERY};\n")

    result = CliRunner().invoke(
        app,
        [
            "export-query",
            "--host",
            parsed.hostname,
            "--port",
            str(parsed.port),
            "--database",
            parsed.path.lstrip("/"),
            "--query-file",
            str(query_file),
            "--folder",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "only reads" in caplog.text
    assert count_rows(postgres_dsn) == 3
