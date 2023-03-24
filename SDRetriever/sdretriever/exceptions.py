""" exceptions module. """


class FileNotFound(Exception):
    """Error raised when file is not found"""


class FileAlreadyExists(Exception):
    """Error raised when there is already the same file in DevCloud"""
