from base.aws.s3 import S3ClientFactory, S3Controller, S3ControllerFactory
from sdretriever.models import S3ObjectRCC, S3ObjectDevcloud
from sdretriever.exceptions import S3UploadError
from sdretriever.config import SDRetrieverConfig
import logging
import gzip
from typing import Iterable

_logger = logging.getLogger(__file__)


class S3DownloaderUploader:
    """Class responsible for every Input/Output opertation of S3"""

    def __init__(self, rcc_s3_controller_factory: S3ControllerFactory,
                 s3_controller: S3Controller, rcc_bucket: str, devcloud_raw_bucket: str, sdr_config: SDRetrieverConfig):
        self.__rcc_s3_controller_factory = rcc_s3_controller_factory
        self.__s3_controller = s3_controller
        self.__rcc_bucket = rcc_bucket
        self.__raw_bucket = devcloud_raw_bucket
        self.__tmp_bucket = sdr_config.temporary_bucket

    def download_from_rcc(self, metadata_chunk_paths: Iterable[str]) -> list[S3ObjectRCC]:
        """
        Download files from RCC S3.
        If the the filename ends with .zip extension it will also decompress it and return the content within.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks

        Returns:
            chunks (list[S3Object]): List with all raw chunks.
        """

        chunks: list[S3ObjectRCC] = []

        for file_path in metadata_chunk_paths:
            downloaded_data = self.__rcc_s3_controller_factory().download_file(self.__rcc_bucket, file_path)

            if file_path.endswith('.zip'):
                downloaded_data = gzip.decompress(downloaded_data)

            chunks.append(
                S3ObjectRCC(
                    data=downloaded_data,
                    s3_key=file_path,
                    bucket=self.__rcc_bucket))

        return chunks

    def __upload_to_devcloud(self, bucket: str, s3_key: str, data: bytes):
        """
        Uploads files to the devcloud

        Args:
            bucket (str): The bucket to upload to
            s3_key (str): The s3 key
            data (bytes): The data to be uploaded

        Raises:
            S3UploadError: If the file already exists
        """
        if self.__s3_controller.check_s3_file_exists(self.__raw_bucket, data.filename):
            _logger.error("File %s already exists in %s", data.filename, self.__raw_bucket)
            raise S3UploadError

        self.__s3_controller.upload_file(data, bucket, s3_key)

    def upload_to_devcloud_raw(self, data: S3ObjectDevcloud) -> str:
        """
        Upload files to the DevCloud raw bucket.
        If the file already exists an error will be thrown.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks

        Returns:
            s3_key (str): The path to where it got uploaded.
        """
        s3_key = f"{data.tenant}/{data.filename}"
        self.__upload_to_devcloud(self.__raw_bucket, s3_key, data.data)

    def upload_to_devcloud_tmp(self, data: S3ObjectDevcloud) -> str:
        """
        Upload files to the DevCloud temporary bucket.
        If the file already exists an error will be thrown.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks

        Returns:
            s3_key (str): The path to where it got uploaded.
        """
        s3_key = f"{data.tenant}/{data.filename}"
        self.__upload_to_devcloud(self.__tmp_bucket, s3_key, data.data)
