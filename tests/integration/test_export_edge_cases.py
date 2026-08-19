import psycopg
import pyarrow.parquet as pq
import pytest

from pg2pyrquet.export import export_to_parquet
from pg2pyrquet.utils.postgres import get_database_tables

pytestmark = pytest.mark.integration

BATCH_SIZE_BYTES = 16 * 1024 * 1024
ROW_GROUP_SIZE = 1_048_576


def export(dsn, output_file, query, batch_size_bytes=BATCH_SIZE_BYTES):
    """
    Runs an export with the defaults every test here shares.
    """
    export_to_parquet(
        dsn=dsn,
        output_file=output_file,
        query=query,
        batch_size_bytes=batch_size_bytes,
        row_group_size=ROW_GROUP_SIZE,
    )


def test_empty_result_writes_a_readable_file(
    postgres_dsn, seeded_table, tmp_path
):
    output_file = tmp_path / "empty.parquet"

    export(
        dsn=postgres_dsn,
        output_file=output_file,
        query=f"SELECT * FROM {seeded_table} WHERE id < 0",
    )

    table = pq.read_table(output_file)

    assert table.num_rows == 0
    assert table.column_names == [
        "id",
        "name",
        "amount",
        "created_at",
        "payload",
        "ref",
        "tags",
    ]


def test_large_result_spans_several_batches(postgres_dsn, tmp_path):
    output_file = tmp_path / "large.parquet"

    export(
        dsn=postgres_dsn,
        output_file=output_file,
        query="SELECT generate_series(1, 200000) AS n",
        batch_size_bytes=4096,
    )

    parquet_file = pq.ParquetFile(output_file)

    assert parquet_file.metadata.num_rows == 200000
    assert parquet_file.metadata.num_row_groups > 1


def test_custom_query_with_limit_and_semicolon(
    postgres_dsn, seeded_table, tmp_path
):
    output_file = tmp_path / "custom.parquet"

    export(
        dsn=postgres_dsn,
        output_file=output_file,
        query=f"SELECT id FROM {seeded_table} ORDER BY id LIMIT 1;",
    )

    assert pq.read_table(output_file).to_pylist() == [{"id": 1}]


def test_every_table_gets_its_own_file(postgres_dsn, seeded_table, tmp_path):
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS second_table;")
        conn.execute("CREATE TABLE second_table (id int);")
        conn.execute("INSERT INTO second_table VALUES (7);")

    try:
        for table in get_database_tables(dsn=postgres_dsn):
            export(
                dsn=postgres_dsn,
                output_file=tmp_path / f"{table}.parquet",
                query=f'SELECT * FROM "{table}"',
            )

        assert (tmp_path / f"{seeded_table}.parquet").exists()
        assert pq.read_table(
            tmp_path / "second_table.parquet"
        ).to_pylist() == [{"id": 7}]
    finally:
        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS second_table;")
