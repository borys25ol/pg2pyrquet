class DirectoryDoesNotExistError(Exception):
    """
    Raised when a specified directory does not exist.
    """


class DirectoryIsAFileError(Exception):
    """
    Raised when a path should be a directory but is a file.
    """


class DatabaseConnectionError(Exception):
    """
    Raised when a specified database does not exist.
    """


class InvalidPostgresCredentialsError(Exception):
    """
    Raised when invalid PostgreSQL credentials are provided.
    """


class TableDoesNotExistError(Exception):
    """
    Raised when a specified table does not exist in the database.
    """


class QueryFileDoesNotExistError(Exception):
    """
    Raised when a specified query file does not exist.
    """


class QueryFileIsADirectoryError(Exception):
    """
    Raised when a path should be a file but is a directory.
    """


class InvalidQueryError(Exception):
    """
    Raised when an invalid query is provided.
    """
