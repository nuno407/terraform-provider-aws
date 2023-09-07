"""Exceptions module for Healtcheck component."""


class FailedHealthCheckError(Exception):
    """Base exception for all the errors that could be raised during healthcheck verifications."""
    artifact_id: str
    message: str

    def __init__(self, artifact_id: str, message: str) -> None:
        self.artifact_id = artifact_id
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"Artifact ID [{self.artifact_id}] {self.message}"


class FailDocumentValidation(FailedHealthCheckError):
    """Error raised when the documentin MongoDB fails to be validated."""

    def __init__(self, artifact_id: str, message: str, json_path: str = "") -> None:
        super().__init__(artifact_id, message)
        self.json_path = json_path


class NotYetIngestedError(FailedHealthCheckError):
    """Error raised when artifact is not ingested yet."""


class NotPresentError(FailedHealthCheckError):
    """Error raised when an artifact is not present."""


class RawFileNotPresent(NotPresentError):
    """Error raised when raw file not present in blob storage."""


class VoxelEntryNotPresent(NotPresentError):
    """Error raised when Voxel51 entry is not present."""


class VoxelEntryNotUnique(NotPresentError):
    """Error raised when multiple Voxel51 entries are present."""


class AnonymizedFileNotPresent(NotPresentError):
    """Error raised when Anonymized file is not present."""


class InvalidMessagePanic(Exception):
    """Error raised when an invalid message is received and no receipt handle is available."""


class ArtifactHandlerNotAvailable(Exception):
    """Error raised when an artifact does not have an healthcheck handler."""
