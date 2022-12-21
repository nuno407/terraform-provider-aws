"""Blob storage controller."""
import logging
from kink import inject

from mypy_boto3_s3 import S3Client
from healthcheck.exceptions import RawFileNotPresent, AnonymizedFileNotPresent
from healthcheck.model import Artifact, S3Params
from botocore.errorfactory import ClientError

_logger: logging.Logger = logging.getLogger(__name__)

@inject
class S3Controller():
    """Blob storage healthcheck controller."""


    def __init__(
            self,
            s3_params: S3Params,
            s3_client: S3Client):
        self.__s3_client = s3_client
        self.__s3_params = s3_params

    def _full_s3_path(self, file_key: str) -> str:
        """
        Given an s3 file name, appends the root folder to the key.

        Args:
            file_key (str): name of the file.

        Returns:
            str: The S3 key fo the file requested
        """
        return f"{self.__s3_params.s3_dir}/{file_key}"

    def _check_s3_file_exists(self, bucket: str, path: str) -> bool:
        """Check if S3 file exists in given bucket and path

        Args:
            bucket (str): S3 bucket
            path (str): S3 key path

        Returns:
            bool: True if exists
        """
        _logger.info("Checking if object exists in bucket: %s path: %s", bucket, path)

        try:
            response = self.__s3_client.head_object(
                Bucket=bucket,
                Key=path
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False

            return True
        except ClientError as err:
            _logger.error(err)
            return False

    def is_s3_anonymized_file_present_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """
        Check for the presence of file in anonymize S3.
        If it doesn't exist, raises an exception.

        Args:
            file_name (str): The file name to be searched (This is not the full key)
            artifact (Artifact): artifact message
        Raises:
            AnonymizedFileNotPresent: If file is not present in the anonymize bucket.
        """
        if not self._is_s3_anonymized_file_present(file_name):
            raise AnonymizedFileNotPresent(artifact, f"Anonymized file {file_name} not found")

    def _is_s3_anonymized_file_present(self, file_name: str) -> bool:
        """
        Check for the presence of file in anonymize S3.

        Args:
            file_name (str): The file name to be searched (This is not the full key)

        Returns:
            bool: True if file exists, False otherwise.
        """
        return self._check_s3_file_exists(self.__s3_params.s3_bucket_anon, self._full_s3_path(file_name))

    def is_s3_raw_file_presence_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """
        Check for the presence of file in raw S3.
        If it doesn't exist, raises an exception.

        Args:
            file_name (str): The file name to be searched (This is not the full key)

        Raises:
            RawFileNotPresent: If file is not present in the anonymize bucket.
        """
        if not self._is_s3_raw_file_presence(file_name):
            raise RawFileNotPresent(artifact, f"Raw file {file_name} not found")

    def _is_s3_raw_file_presence(self, file_name: str) -> bool:
        """
        Check for the presence of file in raw S3.

        Args:
            file_name (str): The file name to be searched (This is not the full key)

        Returns:
            bool: True if file exists, False otherwise.
        """
        return self._check_s3_file_exists(self.__s3_params.s3_bucket_raw, self._full_s3_path(file_name))
