import os
from typing import Iterator

import psycopg
import pytest

TEST_DSN_VARIABLE = "TEST_POSTGRES_DSN"
DEFAULT_TEST_DSN = (
    "postgresql://postgres:postgres@localhost:5433/pg2pyrquet_test"
)

CREATE_TABLE_QUERY = """
    CREATE TABLE {table} (
        id          int,
        name        text,
        amount      numeric(12,2),
        created_at  timestamptz,
        payload     jsonb,
        ref         uuid,
        tags        text[]
    );
"""

INSERT_ROWS_QUERY = """
    INSERT INTO {table} VALUES
    (
        1, 'first', 10.50, '2026-01-01T10:00:00Z', '{{"a": 1}}',
        '11111111-1111-1111-1111-111111111111', ARRAY['x', 'y']
    ),
    (2, NULL, NULL, NULL, NULL, NULL, NULL);
"""


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    """
    Returns a DSN for a reachable test database, or skips the test.
    """
    dsn = os.getenv(TEST_DSN_VARIABLE, DEFAULT_TEST_DSN)

    try:
        with psycopg.connect(dsn, connect_timeout=3):
            return dsn
    except psycopg.OperationalError as error:
        pytest.skip(f"No Postgres reachable at {dsn}: {error}")


@pytest.fixture
def seeded_table(postgres_dsn: str) -> Iterator[str]:
    """
    Creates a table covering every type under test and drops it afterwards.
    """
    table = "types_probe"

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table};")
        conn.execute(CREATE_TABLE_QUERY.format(table=table))
        conn.execute(INSERT_ROWS_QUERY.format(table=table))

    yield table

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table};")
