"""Exceptions"""


class InvalidFileNameException(Exception):
    """Exception raised when compact MDF is invalid."""


class HandlerTypeNotExist(Exception):
    """Exception raised when compact MDF is invalid."""


class InvalidMessage(Exception):
    """Exception raised when compact MDF is invalid."""


class NoProcessingSuccessfulException(Exception):
    """Exception raised when no processing was successful."""


class FailToParseIMU(Exception):
    """Exception raised when there is a problem processing IMU"""
