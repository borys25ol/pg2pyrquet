import pyarrow as pa
from pyarrow import Schema, array, record_batch
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.logging import get_logger

logger = get_logger(name=__name__)


def write_batch_to_parquet(
    writer: ParquetWriter,
    fields_types: dict,
    data: dict[str, list],
    schema: Schema,
) -> None:
    """
    Writes a batch of data to a Parquet file using the provided writer.

    Args:
        writer (ParquetWriter): The ParquetWriter instance used to write data.
        fields_types (dict[str, pa.DataType]): A dictionary mapping column names to their data types.
        data (dict[str, list]): A dictionary where keys are column names and values are lists of column data.
        schema (Schema): The schema defining the structure of the Parquet file.

    Returns:
        None
    """
    batch = record_batch(
        data=[
            array(obj=data[field], type=fields_types[field])
            for field in fields_types.keys()
        ],
        schema=schema,
    )
    writer.write_batch(batch=batch)
