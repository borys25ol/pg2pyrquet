class Pg2ParquetError(Exception):
    """
    Base class for every failure the user can act on.
    """


class DirectoryDoesNotExistError(Pg2ParquetError):
    """
    Raised when a specified directory does not exist.
    """


class DirectoryIsAFileError(Pg2ParquetError):
    """
    Raised when a path should be a directory but is a file.
    """


class DatabaseConnectionError(Pg2ParquetError):
    """
    Raised when a specified database does not exist.
    """


class InvalidPostgresCredentialsError(Pg2ParquetError):
    """
    Raised when invalid PostgreSQL credentials are provided.
    """


class TableDoesNotExistError(Pg2ParquetError):
    """
    Raised when a specified table does not exist in the database.
    """


class QueryFileDoesNotExistError(Pg2ParquetError):
    """
    Raised when a specified query file does not exist.
    """


class QueryFileIsADirectoryError(Pg2ParquetError):
    """
    Raised when a path should be a file but is a directory.
    """


class InvalidQueryError(Pg2ParquetError):
    """
    Raised when an invalid query is provided.
    """


class MissingDatabaseError(Pg2ParquetError):
    """
    Raised when neither a database name nor a DSN was provided.
    """
