from unittest.mock import MagicMock

import pyarrow as pa

from pg2pyrquet.utils.parquet import write_batch_to_parquet


def test_write_batch_to_parquet():
    writer = MagicMock()
    fields_types = {"field1": pa.int32(), "field2": pa.string()}
    data = {"field1": [1, 2], "field2": ["a", "b"]}
    schema = pa.schema(
        fields=[
            pa.field("field1", pa.int32()),
            pa.field("field2", pa.string()),
        ]
    )

    write_batch_to_parquet(
        writer=writer, fields_types=fields_types, data=data, schema=schema
    )
    writer.write_batch.assert_called_once()
