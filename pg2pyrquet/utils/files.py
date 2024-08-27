from pathlib import Path

from pg2pyrquet.core.exceptions import InvalidQueryError


def read_query_from_file(query_path: Path) -> str:
    """
    Reads the query from the specified file path.

    Args:
        query_path (str): The path to the query file.

    Returns:
        str: The contents of the query file.

    Raises:
        InvalidQueryError: If the query is invalid.
    """
    with open(query_path) as file:
        query = file.read()

    if "select" not in query.lower():
        raise InvalidQueryError("Query must contain a SELECT statement.")

    return query
