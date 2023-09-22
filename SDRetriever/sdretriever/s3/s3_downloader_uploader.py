from base.aws.s3 import S3Controller, S3ControllerFactory
from sdretriever.models import S3ObjectRCC, S3ObjectDevcloud
from sdretriever.exceptions import S3UploadError
from sdretriever.config import SDRetrieverConfig
from kink import inject
from typing import Optional, cast
import gzip
from typing import Iterable


@inject
class S3DownloaderUploader:
    """Class responsible for every Input/Output opertation of S3"""

    def __init__(
            self,
            rcc_s3_controller_factory: S3ControllerFactory,
            s3_controller: S3Controller,
            rcc_bucket: str,
            devcloud_raw_bucket: str,
            sdr_config: SDRetrieverConfig):
        self.__rcc_s3_controller_factory = rcc_s3_controller_factory
        self.__devcloud_s3_controller = s3_controller
        self.__rcc_bucket = rcc_bucket
        self.__raw_bucket = devcloud_raw_bucket
        self.__sdr_config = sdr_config

    def download_from_rcc(self, s3_keys: Iterable[str],
                          bucket: Optional[str] = None) -> list[S3ObjectRCC]:
        """
        Download files from RCC S3.
        If the the filename ends with .zip extension it will also decompress it and return the content within.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks
            bucket (Optional[str]): A bucket to download from, if none is provided, will download from the rcd-device-data.

        Returns:
            chunks (list[S3Object]): List with all raw chunks.
        """
        if bucket is None:
            bucket = self.__rcc_bucket

        bucket = cast(str, bucket)
        chunks: list[S3ObjectRCC] = []

        for file_path in s3_keys:
            downloaded_data = self.__rcc_s3_controller_factory().download_file(bucket, file_path)

            if file_path.endswith('.zip'):
                downloaded_data = gzip.decompress(downloaded_data)

            chunks.append(
                S3ObjectRCC(
                    data=downloaded_data,
                    s3_key=file_path,
                    bucket=bucket))

        return chunks

    def __upload_to_devcloud(self, bucket: str, s3_key: str, data: bytes) -> str:
        """
        Uploads files to the devcloud

        Args:
            bucket (str): The bucket to upload to
            s3_key (str): The s3 key
            data (bytes): The data to be uploaded

        Raises:
            S3UploadError: If the file already exists

        Returns:
            s3_path (str): The path to where it got uploaded.
        """

        if self.__sdr_config.discard_already_ingested and self.__devcloud_s3_controller.check_s3_file_exists(
                bucket, s3_key):
            raise S3UploadError(f"File {s3_key} already exists in {bucket}")

        self.__devcloud_s3_controller.upload_file(data, bucket, s3_key)

        return f"s3://{bucket}/{s3_key}"

    def upload_to_devcloud_raw(self, data: S3ObjectDevcloud) -> str:
        """
        Upload files to the DevCloud raw bucket.
        If the file already exists an error will be thrown.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks

        Returns:
            s3_path (str): The path to where it got uploaded.
        """
        s3_key = f"{data.tenant}/{data.filename}"
        return self.__upload_to_devcloud(self.__raw_bucket, s3_key, data.data)

    def upload_to_devcloud_tmp(self, data: S3ObjectDevcloud) -> str:
        """
        Upload files to the DevCloud temporary bucket.
        If the file already exists an error will be thrown.

        Args:
            metadata_chunk_paths (Iterable[str]): An Iterable containing all the meta chunks

        Returns:
            s3_path (str): The path to where it got uploaded.
        """
        s3_key = f"{data.tenant}/{data.filename}"
        return self.__upload_to_devcloud(self.__sdr_config.temporary_bucket, s3_key, data.data)

    def is_file_devcloud_tmp(self, filename: str, tenant: str) -> bool:
        """
        Returns true if the file is already available in DevCloud temporary bucket.

        Args:
            filename (str): The filename in S3 (without paths)
            tenant (str): The tenant in S3

        Returns:
            bool: True if exists, false otherwise
        """
        s3_key = f"{tenant}/{filename}"
        return self.__devcloud_s3_controller.check_s3_file_exists(
            self.__sdr_config.temporary_bucket, s3_key)

    def is_file_devcloud_raw(self, filename: str, tenant: str) -> bool:
        """
        Returns true if the file is already available in DevCloud raw bucket.

        Args:
            filename (str): The filename in S3 (without paths)
            tenant (str): The tenant in S3

        Returns:
            bool: True if exists, false otherwise
        """
        s3_key = f"{tenant}/{filename}"
        return self.__devcloud_s3_controller.check_s3_file_exists(self.__raw_bucket, s3_key)
