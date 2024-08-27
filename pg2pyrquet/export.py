from collections import defaultdict
from pathlib import Path

import psycopg
import pyarrow as pa
from psycopg.rows import dict_row
from pyarrow.parquet import ParquetWriter

from pg2pyrquet.core.logging import get_logger
from pg2pyrquet.utils.parquet import write_batch_to_parquet
from pg2pyrquet.utils.postgres import get_query_data_types

logger = get_logger(name=__name__)


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


def export_to_parquet(
    dsn: str, output_file: Path, batch_size: int, query: str
) -> None:
    """
    Processes export the specified table from the database to a Parquet file.

    Args:
        dsn (str): The Data Source Name for connecting to the PostgreSQL database.
        output_file (Path): The path to the output Parquet file.
        batch_size (int): The number of rows to process in each batch.
        query (str): SQL query to execute.
    """
    records = defaultdict(list)

    data_types = get_query_data_types(dsn=dsn, query=query)
    schema = pa.schema(fields=data_types)

    with ParquetWriter(where=output_file, schema=schema) as writer:
        with psycopg.connect(dsn) as conn:
            logger.info("Connected to DB, starting to execute query...")

            with conn.cursor(
                name="pg-to-parquet", row_factory=dict_row
            ) as cur:
                cur.itersize = batch_size
                cur.execute(query)
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
