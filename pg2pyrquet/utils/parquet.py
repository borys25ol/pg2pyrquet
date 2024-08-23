from pyarrow import Schema, array, record_batch
from pyarrow.parquet import ParquetWriter


def write_batch_to_parquet(
    writer: ParquetWriter,
    fields_types: dict,
    data: dict[str, list],
    schema: Schema,
) -> None:
    batch = record_batch(
        data=[
            array(obj=data[field], type=fields_types[field])
            for field in fields_types.keys()
        ],
        schema=schema,
    )
    writer.write_batch(batch=batch)
