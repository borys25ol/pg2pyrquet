class DirectoryDoesNotExistError(Exception):
    """
    Raised when a specified directory does not exist.
    """


class DirectoryIsAFileError(Exception):
    """
    Raised when a specified path is expected to be a directory but is actually a file.
    """


class DatabaseDoesNotExistError(Exception):
    """
    Raised when a specified database does not exist.
    """


class TableDoesNotExistError(Exception):
    """
    Raised when a specified table does not exist in the database.
    """
