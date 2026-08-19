from urllib.parse import urlparse

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from pg2pyrquet.__main__ import app

pytestmark = pytest.mark.integration

runner = CliRunner()


def test_export_table_writes_a_readable_file(
    postgres_dsn, seeded_table, tmp_path, monkeypatch
):
    parsed = urlparse(postgres_dsn)
    monkeypatch.setenv("POSTGRES_USER", parsed.username or "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", parsed.password or "postgres")

    result = runner.invoke(
        app,
        [
            "export-table",
            "--host",
            parsed.hostname,
            "--port",
            str(parsed.port),
            "--database",
            parsed.path.lstrip("/"),
            "--table",
            seeded_table,
            "--folder",
            str(tmp_path),
            "--output-file",
            "cli.parquet",
        ],
    )

    assert result.exit_code == 0, result.output
    assert pq.read_table(tmp_path / "cli.parquet").num_rows == 2
