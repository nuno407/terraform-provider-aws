"""Exceptions for the consumer module."""


class NotSupportedArtifactError(Exception):
    def __init__(self, message):
        super().__init__(message)


class SnapshotNotFound(Exception):
    """The snapshot was not found"""
