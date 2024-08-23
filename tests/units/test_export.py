from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from pg2pyrquet.export import export_to_parquet, reset_column_values


def test_reset_column_values():
    fields_types = {"field1": pa.int32(), "field2": pa.string()}
    records = {"field1": [1, 2], "field2": ["a", "b"]}
    reset_column_values(fields_types=fields_types, records=records)
    assert records == {"field1": [], "field2": []}


@patch("pg2pyrquet.export.ParquetWriter")
@patch("pg2pyrquet.export.reset_column_values")
@patch("pg2pyrquet.utils.postgres.psycopg.connect")
@patch("pg2pyrquet.utils.parquet.write_batch_to_parquet")
def test_process_export_to_parquet(
    mock_parquet_writer,
    mock_reset_column_values,
    mock_psycopg_connect,
    mock_write_batch_to_parquet,
):
    mock_writer = MagicMock()
    mock_parquet_writer.return_value.__enter__.return_value = mock_writer

    mock_cursor = MagicMock()
    mock_cursor.__iter__.return_value = [
        {"field1": 1, "field2": "a"},
        {"field1": 2, "field2": "b"},
    ]
    mock_cursor.itersize = 1
    mock_psycopg_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
        mock_cursor
    )

    dsn = "dsn"
    table = "test_table"
    output_file = Path("./data/pytest.parquet")
    batch_size = 1
    data_types = {"field1": pa.int32(), "field2": pa.string()}

    export_to_parquet(
        dsn=dsn,
        table=table,
        output_file=output_file,
        batch_size=batch_size,
        data_types=data_types,
    )

    # Check if the writer was called to write batches
    mock_write_batch_to_parquet.assert_called()
    # Check if reset_column_values was called
    mock_reset_column_values.assert_called()
