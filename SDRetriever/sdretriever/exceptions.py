""" exceptions module. """


class FileNotFound(Exception):
    """Error raised when file is not found"""


class TemporaryIngestionError(Exception):
    """Error raised when there is an error during ingestion and it should be tried later"""


class S3UploadError(Exception):
    """Error raised when there is an error during S3 upload"""


class S3FileNotFoundError(TemporaryIngestionError):
    """Error raised when file is not found"""


class S3DownloadError(TemporaryIngestionError):
    """Error raised when there is an error during S3 download"""


class UploadNotYetCompletedError(TemporaryIngestionError):
    """Error raised when upload is not yet completed"""


class EmptyFileError(Exception):
    """Error raised when metadata is empty"""


class NoIngestorForArtifactError(Exception):
    """Error raised when there is no ingestor for the artifact"""
