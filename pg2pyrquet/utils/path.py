from pathlib import Path

from pg2pyrquet.core.exceptions import (
    DirectoryDoesNotExistError,
    DirectoryIsAFileError,
    OutputFileExistsError,
    QueryFileDoesNotExistError,
    QueryFileIsADirectoryError,
)


def validate_output_path(output_path: str | Path) -> Path:
    """
    Validates the `output_path` ensuring it exists and is a directory.

    Args:
        output_path (str | Path): The path to validate.

    Returns:
        Path: The validated output path as a Path object.

    Raises:
        DirectoryDoesNotExistError: If the path does not exist.
        DirectoryIsAFileError: If the path is a file.
    """
    output_path = Path(output_path)

    if not output_path.exists():
        raise DirectoryDoesNotExistError(
            f"Output directory '{output_path}' does not exist."
        )

    if output_path.is_file():
        raise DirectoryIsAFileError(
            f"Output directory '{output_path}' is actually a file."
        )

    return output_path


def validate_query_path(query_path: str | Path) -> Path:
    """
    Validates the `query_path` ensuring it exists and is a file.

    Args:
        query_path (str | Path): The path to validate.

    Returns:
        Path: The validated query path as a Path object.

    Raises:
        QueryFileDoesNotExistError: If the path does not exist.
        QueryFileIsADirectoryError: If the path is a directory
    """
    query_path = Path(query_path)

    if not query_path.exists():
        raise QueryFileDoesNotExistError(
            f"Query file '{query_path}' does not exist."
        )

    if query_path.is_dir():
        raise QueryFileIsADirectoryError(
            f"Query file '{query_path}' is actually a directory."
        )

    return query_path


def validate_output_file(output_file: Path, overwrite: bool) -> Path:
    """
    Refuses to replace an existing file unless overwriting was asked for.

    Args:
        output_file (Path): The file the export would write.
        overwrite (bool): Whether replacing an existing file is allowed.

    Returns:
        Path: The validated output file.

    Raises:
        OutputFileExistsError: If the file exists and overwrite is False.
    """
    if output_file.exists() and not overwrite:
        raise OutputFileExistsError(
            f"Output file '{output_file}' already exists."
            " Pass --overwrite to replace it."
        )

    return output_file
