"""Blob storage controller."""

import logging

from botocore.errorfactory import ClientError
from kink import inject
from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)

@inject
class S3Controller: # pylint: disable=too-few-public-methods
    """Blob storage healthcheck controller."""

    def __init__(
            self,
            s3_client: S3Client):
        self.__s3_client = s3_client

    def check_s3_file_exists(self, bucket: str, path: str) -> bool:
        """Check if S3 file exists in given bucket and path

        Args:
            bucket (str): S3 bucket
            path (str): S3 key path

        Returns:
            bool: True if exists
        """
        _logger.info(
            "Checking if object exists in bucket: %s path: %s", bucket, path)
        try:
            response = self.__s3_client.head_object(
                Bucket=bucket,
                Key=path
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                return False

            return True
        except ClientError as err:
            _logger.exception(err)
            return False
