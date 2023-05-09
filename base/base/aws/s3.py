"""Blob storage controller."""

import logging
from typing import Callable
import re

from botocore.errorfactory import ClientError
from kink import inject
from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)

CONTENT_TYPES = {
    "json": "application/json",  # pylint: disable=duplicate-code
    "mp4": "video/mp4",  # pylint: disable=duplicate-code
    "avi": "video/x-msvideo",  # pylint: disable=duplicate-code
    "txt": "text/plain",  # pylint: disable=duplicate-code
    "webm": "video/webm",  # pylint: disable=duplicate-code
    "jpeg": "image/jpeg",  # pylint: disable=duplicate-code
    "csv": "text/plain"  # pylint: disable=duplicate-code
}


@inject
class S3Controller:  # pylint: disable=too-few-public-methods
    """Blob storage healthcheck controller."""

    def __init__(
            self,
            s3_client: S3Client):
        self.__s3_client = s3_client

    def check_s3_directory_exists(self, bucket: str, prefix: str) -> bool:
        """Check if S3 directory exists in given bucket and prefix

        Args:
            bucket (str): S3 bucket
            prefix (str): S3 key prefix

        Returns:
            bool: True if exists
        """
        try:
            response = self.__s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                return False

            return True
        except ClientError:
            return False

    def download_file(self, s3_bucket: str, path: str) -> bytes:
        """Retrieves a given file from the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            s3_bucket {string} -- [name of the source s3 bucket]
            path {string} -- [string containg the path + file name of
                                   the target file to be downloaded
                                   from the source s3 bucket
                                   (e.g. "uber/test_file_s3.txt")]
        Returns:
            object_file {bytes} -- [downloaded file in bytes format]
        """
        full_path = s3_bucket + "/" + path
        _logger.debug("Downloading [%s]..", full_path)
        response = self.__s3_client.get_object(
            Bucket=s3_bucket,
            Key=path
        )

        object_file = response["Body"].read()

        _logger.debug("Downloaded [%s]", full_path)

        return object_file

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
        except ClientError:
            return False

    def upload_file(self, file: bytes, bucket: str, path: str) -> None:
        """Stores a given file in the selected s3 bucket

        Arguments:
            file {bytes} -- [file to be uploaded to target S3 bucket]
            bucket {string} -- [name of the destination s3 bucket]
            path {string} -- [string containg the path + file name to be
                                  used for the file in the destination s3
                                  bucket (e.g. "uber/test_file_s3.txt")]
        """
        _logger.info(
            "Uploading file to bucket: %s path: %s", bucket, path)
        file_extension = path.split(".")[-1]

        self.__s3_client.put_object(
            Body=file,
            Bucket=bucket,
            Key=path,
            ServerSideEncryption="aws:kms",
            ContentType=CONTENT_TYPES.get(
                file_extension, "application/octet-stream")
        )

        _logger.info("Uploaded [%s]", path)

    @staticmethod
    def get_s3_path_parts(raw_path: str) -> tuple[str, str]:
        """
        Splits a full s3 path into bucket and key

        Args:
            raw_path (str): _description_

        Raises:
            ValueError: If raw_path is not a full an s3 path.

        Returns:
            tuple[str, str]: The bucket and the key respectively
        """
        match = re.match(r"^s3://([^/]+)/(.*)$", raw_path)

        if match is None or len(match.groups()) != 2:
            raise ValueError("Invalid path: " + raw_path)

        bucket = match.group(1)
        key = match.group(2)
        return bucket, key


S3ControllerFactory = Callable[[], S3Controller]
S3ClientFactory = Callable[[], S3Client]
