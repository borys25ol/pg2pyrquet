from pathlib import Path

from pg2pyrquet.core.exceptions import InvalidQueryError


def read_query_from_file(query_path: Path) -> str:
    """
    Reads the query from the specified file path.

    The contents are not inspected beyond being non-empty. Whether the
    query only reads is enforced by the server, which runs every export
    in a read-only transaction. A keyword check cannot do that job: it
    accepts `WITH x AS (DELETE ...) SELECT ...` and rejects valid forms
    such as `TABLE orders`.

    Args:
        query_path (Path): The path to the query file.

    Returns:
        str: The contents of the query file.

    Raises:
        InvalidQueryError: If the file is empty.
    """
    query = query_path.read_text()

    if not query.strip():
        raise InvalidQueryError(f"Query file '{query_path}' is empty.")

    return query
