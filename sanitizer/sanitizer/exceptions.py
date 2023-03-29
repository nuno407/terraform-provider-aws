"""Contains all custom exceptions used by the sanitizer."""

class SanitizerException(Exception):
    """Base class for all sanitizer exceptions."""

class MessageException(SanitizerException):
    """Base class for all message related exceptions."""

class ArtifactException(SanitizerException):
    """Base class for all artifact related exceptions."""

class InvalidMessagePanic(MessageException):
    """Error raised when an invalid message is received and no receipt handle is available."""

class InvalidMessageError(MessageException):
    """Error raised when an invalid message is received."""
