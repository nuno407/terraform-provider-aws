""" exceptions module. """


class FileNotFound(Exception):
    """Error raised when file is not found"""


class FileAlreadyExists(Exception):
    """Error raised when there is already the same file in DevCloud"""


class TemporaryIngestionError(Exception):
    """Error raised when there is an error during ingestion and it should be tried later"""


class KinesisDownloadError(TemporaryIngestionError):
    """Error raised when there is an error during Kinesis download"""


class S3UploadError(TemporaryIngestionError):
    """Error raised when there is an error during S3 upload"""


class S3FileNotFoundError(TemporaryIngestionError):
    """Error raised when file is not found"""


class UploadNotYetCompletedError(TemporaryIngestionError):
    """Error raised when upload is not yet completed"""


class NoIngestorForArtifactError(Exception):
    """Error raised when there is no ingestor for the artifact"""
