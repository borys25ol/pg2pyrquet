import datetime

import pyarrow.parquet as pq
import pytest

from pg2pyrquet.export import export_to_parquet

pytestmark = pytest.mark.integration


def test_exports_basic_types(postgres_dsn, seeded_table, tmp_path):
    output_file = tmp_path / "basic.parquet"

    export_to_parquet(
        dsn=postgres_dsn,
        output_file=output_file,
        batch_size=1000,
        query=f"SELECT id, name, created_at, tags FROM {seeded_table} ORDER BY id",
    )

    rows = pq.read_table(output_file).to_pylist()

    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "first"
    assert rows[0]["created_at"] == datetime.datetime(
        2026, 1, 1, 10, 0, tzinfo=datetime.timezone.utc
    )
    assert rows[0]["tags"] == ["x", "y"]
    assert rows[1] == {
        "id": 2,
        "name": None,
        "created_at": None,
        "tags": None,
    }
