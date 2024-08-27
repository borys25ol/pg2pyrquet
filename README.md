Postgres Exports to Apache Parquet with Python (pg2pyrquet)
====================

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Pre-commit: enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=flat)](https://github.com/pre-commit/pre-commit)

## Description

`pg2pyrquet` is a Python CLI tool designed to export PostgreSQL tables into Parquet files.
This tool is particularly useful for data engineers and analysts who want to efficiently convert PostgreSQL data into a columnar storage format that is optimal for analytical workloads.


## Features

- **Efficient Data Export**: Export PostgreSQL tables directly to Parquet files.
- **Batch Processing**: Specify batch size to handle large datasets efficiently.
- **Customizable Output**: Define output folder and file name for the Parquet file.


## Installation

Setup and activate a python3 virtualenv via your preferred method. e.g. and install production requirements:

```shell
make ve
```


To use `pg2pyrquet`, you need to have Python installed. You can install the necessary dependencies using `pip`:

```sh
pip install -r requirements.txt
```

Configuration
--------------

Replace `.env.example` with real `.env`, changing placeholders

```
POSTGRES_USER=<postgres-user>
POSTGRES_PASSWORD=<postgres-password>
POSTGRES_HOST=<postgres-host>
POSTGRES_PORT=<postgres-port>
```

Or you can set the environment variables directly:

```shell
export POSTGRES_HOST=<host>
export POSTGRES_PORT=<port>
export POSTGRES_USER=<user>
export POSTGRES_PASSWORD=<password>
````

Or using more secure way:

```shell
read -s -p "Enter POSTGRES host: " POSTGRES_HOST || export POSTGRES_HOST

read -s -p "Enter POSTGRES port: " POSTGRES_PORT || export POSTGRES_PORT

read -s -p "Enter POSTGRES User: " POSTGRES_USER || export POSTGRES_USER

read -s -p "Enter POSTGRES Password: " POSTGRES_PASSWORD || export POSTGRES_PASSWORD
```

Usage
-----

The primary command provided by this tool is `export-table` and `export-database` commands,
which allows you to export a PostgreSQL table or all PostgreSQL database tables to a Parquet files.

### Export a Single Table

To export a single table from a PostgreSQL database to a Parquet file, use the `export-table` command.
This command allows you to specify the database, table, output folder, output file name, and the batch size for processing.

#### Example Command:

```shell
python -m pg2pyrquet export-table \
    --database <database_name> \
    --table <table_name> \
    --folder <output_folder> \
    --output-file <output_filename> \
    --batch-size <batch_size>
```

#### Command Options

- `--database`: The name of the PostgreSQL database you want to export data from.
- `--table`: The specific table within the database to export.
- `--folder`: The directory where the Parquet file will be saved.
- `--output-file`: The name of the output Parquet file.
- `--batch-size`: The number of rows to process in each batch. This helps in managing memory usage for large tables.

### Export All Database Tables

To export all tables from a PostgreSQL database to Parquet files, use the `export-database` command.
This command exports each table into a separate Parquet file in the specified output folder.

#### Example Command

```shell
python -m pg2pyrquet export-database \
    --database <database_name> \
    --folder <output_folder> \
    --batch-size <batch_size>
```

#### Command Options

- `--database`: The name of the PostgreSQL database you want to export data from.
- `--folder`: The directory where the Parquet file will be saved.
- `--batch-size`: The number of rows to process in each batch. This helps in managing memory usage for large tables.


#### Note on File Naming
When using the `export-database` command, each Parquet file will be named according to the table name, following the format `{table_name}.parquet`.


### Export a Custom Query

To export the result of a custom SQL query from a PostgreSQL database to a Parquet file, use the `export_query` command.
This command allows you to specify the database, the file containing the SQL query, the output folder, the output file name, and the batch size for processing.

#### Example Command:

```shell
python -m pg2pyrquet export-query \
    --database <database_name> \
    --query-file <query_file_path> \
    --folder <output_folder> \
    --output-file <output_filename> \
    --batch-size <batch_size>
```

#### Command Options

- `--database`: The name of the PostgreSQL database you want to export data from.
- `--query-file`: The path to the file containing the SQL query (like `custom-query.sql`).
- `--folder`: The directory where the Parquet file will be saved.
- `--output-file`: The name of the output Parquet file.
- `--batch-size`: The number of rows to process in each batch. This helps in managing memory usage for large tables.

Example SQL query file (`custom-query.sql`):

```sql
SELECT *
FROM my_table
WHERE column_name = 'value'
GROUP BY column_name
LIMIT 1000;
```

### Running from Python

Also, you have the ability execute all available commands as Python functions:

Create a new file `export.py` with the following content:

```python
from pg2pyrquet import export_database, export_query, export_table


def run_export_database() -> None:
    export_database(
        database="test_database", output_path="./data", batch_size=5000
    )


def run_export_table() -> None:
    export_table(
        database="test_database",
        table="test_table",
        output_path="./data",
        output_file="./data/test_table.parquet",
        batch_size=5000,
    )


def run_export_query() -> None:
    export_query(
        database="test_database",
        query_file="./data/query.sql",
        output_path="./data",
        output_file="./data/query.parquet",
        batch_size=5000,
    )


if __name__ == "__main__":
    run_export_database()
    run_export_table()
    run_export_query()

```

And run it:

```shell
python export.py
```

Contributing
------------
Contributions are welcome!
If you find a bug or have a feature request, please open an issue or submit a pull request on GitHub.
