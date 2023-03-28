"""Contains all custom exceptions used by the sanitizer."""

class InvalidMessagePanic(Exception):
    """Error raised when an invalid message is received and no receipt handle is available."""
