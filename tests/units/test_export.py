from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from pg2pyrquet.export import export_to_parquet, normalise_query

SCHEMA = pa.schema([("field1", pa.int32()), ("field2", pa.string())])


def build_batch(rows: int) -> pa.RecordBatch:
    """
    Builds a record batch matching SCHEMA with the given number of rows.
    """
    return pa.record_batch(
        data=[
            pa.array(list(range(rows)), type=pa.int32()),
            pa.array([f"value-{index}" for index in range(rows)]),
        ],
        schema=SCHEMA,
    )


def run_export(batches: list[pa.RecordBatch]) -> MagicMock:
    """
    Runs the export against a fake driver and returns the writer mock.
    """
    writer = MagicMock()
    reader = MagicMock()
    reader.schema = SCHEMA
    reader.__iter__.return_value = iter(batches)

    cursor = MagicMock()
    cursor.fetch_record_batch.return_value = reader

    with (
        patch("pg2pyrquet.export.ParquetWriter") as writer_class,
        patch("pg2pyrquet.export.connect") as connect,
    ):
        writer_class.return_value.__enter__.return_value = writer
        connection = connect.return_value.__enter__.return_value
        connection.cursor.return_value.__enter__.return_value = cursor

        export_to_parquet(
            dsn="dsn",
            output_file=Path("./data/pytest.parquet"),
            query="SELECT * FROM test_table",
            batch_size_bytes=1024,
            row_group_size=10,
        )

    return writer


def test_writes_every_batch_the_reader_yields():
    writer = run_export(batches=[build_batch(3), build_batch(2)])

    assert [
        call.kwargs["batch"].num_rows
        for call in writer.write_batch.call_args_list
    ] == [3, 2]


def test_passes_the_row_group_size_through():
    writer = run_export(batches=[build_batch(1)])

    assert writer.write_batch.call_args.kwargs["row_group_size"] == 10


def test_writes_nothing_for_an_empty_result():
    writer = run_export(batches=[])

    writer.write_batch.assert_not_called()


def test_normalise_query_strips_trailing_semicolon():
    assert normalise_query(query="SELECT 1;\n") == "SELECT 1"


def test_normalise_query_keeps_inner_semicolons_untouched():
    query = "SELECT ';' AS marker"

    assert normalise_query(query=query) == query
