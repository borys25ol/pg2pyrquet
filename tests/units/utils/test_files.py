from pathlib import Path

import pytest

from pg2pyrquet.core.exceptions import InvalidQueryError
from pg2pyrquet.utils.files import read_query_from_file


def test_read_query_from_file_valid():
    query_path = Path("./test_query.sql")
    query_content = "SELECT * FROM test_table;"
    query_path.write_text(query_content)

    query = read_query_from_file(query_path)
    assert query == query_content

    query_path.unlink()


def test_read_query_from_file_invalid():
    query_path = Path("./invalid_query.sql")
    query_content = "INSERT INTO test_table VALUES (1, 'test');"
    query_path.write_text(query_content)

    with pytest.raises(InvalidQueryError):
        read_query_from_file(query_path)

    query_path.unlink()
