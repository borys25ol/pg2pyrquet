from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from pg2pyrquet.export import export_to_parquet, reset_column_values

DATA_TYPES = {"field1": pa.int32(), "field2": pa.string()}


def build_rows(count: int) -> list[dict]:
    """
    Builds a list of fake database records matching DATA_TYPES.
    """
    return [
        {"field1": index, "field2": f"value-{index}"}
        for index in range(count)
    ]


def run_export(rows: list[dict], batch_size: int) -> list[int]:
    """
    Runs the export against a fake cursor and returns written batch sizes.
    """
    writer = MagicMock()
    cursor = MagicMock()
    cursor.__iter__.return_value = iter(rows)

    with (
        patch("pg2pyrquet.export.ParquetWriter") as writer_class,
        patch(
            "pg2pyrquet.export.get_query_data_types", return_value=DATA_TYPES
        ),
        patch("pg2pyrquet.export.psycopg.connect") as connect,
    ):
        writer_class.return_value.__enter__.return_value = writer
        connection = connect.return_value.__enter__.return_value
        connection.cursor.return_value.__enter__.return_value = cursor

        export_to_parquet(
            dsn="dsn",
            output_file=Path("./data/pytest.parquet"),
            batch_size=batch_size,
            query="SELECT * FROM test_table",
        )

    return [
        call.kwargs["batch"].num_rows
        for call in writer.write_batch.call_args_list
    ]


def test_reset_column_values():
    fields_types = {"field1": pa.int32(), "field2": pa.string()}
    records = {"field1": [1, 2], "field2": ["a", "b"]}
    reset_column_values(fields_types=fields_types, records=records)
    assert records == {"field1": [], "field2": []}


def test_export_writes_full_batches():
    assert run_export(rows=build_rows(count=4), batch_size=2) == [2, 2]


def test_export_writes_remainder_as_last_batch():
    assert run_export(rows=build_rows(count=3), batch_size=2) == [2, 1]


def test_export_writes_single_batch_when_rows_fit():
    assert run_export(rows=build_rows(count=2), batch_size=10) == [2]


def test_export_writes_nothing_for_empty_result():
    assert run_export(rows=[], batch_size=2) == []
