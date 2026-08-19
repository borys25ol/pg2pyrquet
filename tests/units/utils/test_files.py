import pytest

from pg2pyrquet.core.exceptions import InvalidQueryError
from pg2pyrquet.utils.files import read_query_from_file


def write_query(tmp_path, content: str):
    """
    Writes a query file and returns its path.
    """
    query_path = tmp_path / "query.sql"
    query_path.write_text(content)

    return query_path


def test_reads_the_file_verbatim(tmp_path):
    content = "SELECT * FROM test_table;"

    assert read_query_from_file(write_query(tmp_path, content)) == content


def test_accepts_a_cte(tmp_path):
    content = "WITH recent AS (SELECT * FROM events) SELECT * FROM recent;"

    assert read_query_from_file(write_query(tmp_path, content)) == content


def test_accepts_a_query_starting_with_a_comment(tmp_path):
    content = "-- monthly totals\nSELECT count(*) FROM orders;"

    assert read_query_from_file(write_query(tmp_path, content)) == content


def test_rejects_an_empty_file(tmp_path):
    with pytest.raises(InvalidQueryError):
        read_query_from_file(write_query(tmp_path, ""))


def test_rejects_a_file_holding_only_whitespace(tmp_path):
    with pytest.raises(InvalidQueryError):
        read_query_from_file(write_query(tmp_path, "   \n\t\n"))


def test_accepts_a_table_command(tmp_path):
    """
    `TABLE x` is PostgreSQL shorthand for `SELECT * FROM x`, and holds no
    "select" substring for a keyword check to find.
    """
    content = "TABLE orders;"

    assert read_query_from_file(write_query(tmp_path, content)) == content
