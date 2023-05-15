"""Exceptions for the consumer module."""


class NotSupportedArtifactError(Exception):
    def __init__(self, message):
        super().__init__(message)
