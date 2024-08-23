from collections import defaultdict
from pathlib import Path

import psycopg
import pyarrow as pa
from psycopg.rows import dict_row
from pyarrow import Schema, array, record_batch
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.utils.postgres import SELECT_ALL_TABLE_QUERY

logger = get_logger(name=__file__)


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


def reset_column_values(
    fields_types: dict[str, pa.DataType], records: dict[str, list]
) -> None:
    """
    Resets the values list for each column in the records dictionary based on the fields_types.

    Args:
        fields_types (dict[str, pa.DataType]): The dictionary of field names and their data types.
        records (dict[str, list]): The dictionary to reset, where keys are field names.
    """
    for field in fields_types:
        records[field] = []


def process_export_to_parquet(
    dsn: str,
    table: str,
    output_file: Path,
    batch_size: int,
    data_types: dict[str, pa.DataType],
) -> None:
    """
    Processes export the specified table from the database to a Parquet file.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        table (str): The name of the table to dump.
        output_file (Path): The path to the output Parquet file.
        batch_size (int): The number of rows to process in each batch.
        data_types (dict[str, pa.DataType]): A dictionary mapping column names to their data types.
    """
    records = defaultdict(list)
    schema = pa.schema(fields=data_types)

    with ParquetWriter(where=output_file, schema=schema) as writer:
        with psycopg.connect(dsn) as conn:
            logger.info("Connected to DB, starting to execute query...")

            with conn.cursor(
                name="pg-to-parquet", row_factory=dict_row
            ) as cur:
                cur.itersize = batch_size
                cur.execute(SELECT_ALL_TABLE_QUERY.format(table_name=table))
                logger.info("Query executed...")

                for index, record in enumerate(cur):
                    for column, value in record.items():
                        records[column].append(value)

                    if index % batch_size == 0:
                        logger.info(
                            f"Writing batch {index // batch_size + 1} to the file: {output_file}"
                        )
                        write_batch_to_parquet(
                            writer=writer,
                            fields_types=data_types,
                            data=records,
                            schema=schema,
                        )
                        reset_column_values(
                            fields_types=data_types, records=records
                        )

                write_batch_to_parquet(
                    writer=writer,
                    fields_types=data_types,
                    data=records,
                    schema=schema,
                )
                logger.info("Export finished successfully.")
