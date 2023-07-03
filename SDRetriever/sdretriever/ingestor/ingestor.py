""" ingestor module. """
import logging as log
import re
from abc import abstractmethod
from datetime import datetime
from typing import Iterator, Optional, Tuple

from botocore.exceptions import ClientError
from kink import inject

from base.aws.container_services import ContainerServices, RCCS3ObjectParams
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact
from sdretriever.exceptions import S3FileNotFoundError, S3UploadError
from sdretriever.s3_finder import S3Finder

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class Ingestor:
    """Ingestor base class"""

    def __init__(
        self,
        container_services: ContainerServices,
        rcc_s3_client_factory: S3ClientFactory,
        s3_finder: S3Finder,
        s3_controller: S3Controller
    ) -> None:
        """_summary_

        Args:
            container_services (ContainerServices): instance of ContainerServices
            rcc_s3_client_factory (Callable[[], S3Client]): factory that creates
                                                an authenticated S3 client for RCC
        """
        self._container_svcs = container_services
        self._rcc_s3_client_factory = rcc_s3_client_factory
        self._s3_finder = s3_finder
        self._s3_controller = s3_controller

    @abstractmethod
    def ingest(self, artifact: Artifact):
        """Ingests the artifacts described in a message into the DevCloud"""

    def check_if_s3_rcc_path_exists(self,
                                    s3_path: str,
                                    bucket: str,
                                    max_s3_api_calls: int = 1,
                                    exact=False) -> Tuple[bool,
                                                          dict]:
        """Verify if path exists on target S3 bucket.

        - Check if the file exists in a given path.
        - Verifies if the object found size is bigger then 0.
        - If the object is not found, continue to look for tenant and deviceid prefixes
                to provide logging information
        - If the exact argument is true, all objects will still be retrieved but the
                first return value will only be true if one of
                the objects matches th path exactly.

        Args:
            s3_path (str): S3 path to look for.
            bucket (str): S3 bucket name.
            max_s3_api_calls (str): Maximum s3 list api calls to be made
            exact (bool): Only match files with exact name, if false it will only match the prefix
        Returns:
            A tuple containing a boolean response if the path was found and a
                dictionary with the S3 objects information
        """

        # Check if there is a file with the same name already stored on target S3 bucket
        try:
            list_objects_response: dict = dict(ContainerServices.list_s3_objects(
                s3_path, bucket, self._rcc_s3_client, max_iterations=max_s3_api_calls))

            if list_objects_response.get('KeyCount') and int(
                    list_objects_response['Contents'][0]['Size']) == 0:
                return False, list_objects_response

        except Exception:  # pylint: disable=broad-except
            s3_object_params = RCCS3ObjectParams(s3_path=s3_path, bucket=bucket)
            return self._container_svcs.check_if_tenant_and_deviceid_exists_and_log_on_error(
                self._rcc_s3_client, s3_object_params), {}

        if list_objects_response is None or list_objects_response.get('KeyCount', 0) == 0:
            return False, {}

        if exact:
            exact_match = s3_path in [object_dict['Key']
                                      for object_dict in list_objects_response['Contents']]
            return exact_match, list_objects_response

        return True, list_objects_response

    def get_file_in_rcc(
        self,
        bucket: str,
        tenant: str,
        device_id: str,
        prefix: str,
        start_time: datetime,
        end_time: datetime,
        extensions: Optional[list[str]] = None
    ) -> bytes:
        """
        Transverses across the RCC S3 until a file is found according to the prefix given.
        Returns the first match. If the file is not found an exception is raised.

        Args:
            bucket (str): RCC bucket to search on
            tenant (str): Tenant of the artifact
            device_id (str): Device id of the artifact
            prefix (str): Prefix to search for inside the hour folder
            start_time (datetime): The start timestamp to search for in RCC.
            end_time (datetime): The stop timestamp to search for in RCC.
            extensions (str): The extensions to filter for

        Returns:
            bytes: Returns the contents of the file if found.
        """
        if extensions is None:
            extensions = []
        subfolders: Iterator[str] = self._s3_finder.discover_s3_subfolders(
            f'{tenant}/{device_id}/', bucket, self._rcc_s3_client, start_time, end_time)

        for subfolder in subfolders:
            path = subfolder + prefix
            file_found, list_objects_response = self.check_if_s3_rcc_path_exists(path, bucket)

            if not file_found:
                continue

            # Filter the file for any of the extensions provided
            files: list[dict] = list(filter(lambda x: any(x['Key'].endswith(extension)  # type: ignore
                                                          for extension in extensions),
                                            list_objects_response['Contents']))

            if len(files) > 1:
                raise RuntimeError(
                    f"Found more then one file for {path}, files found: {str(files)}")

            if len(files) == 1:
                snapshot_bytes = bytes(self._container_svcs.download_file(
                    self._rcc_s3_client, bucket, files[0]['Key']))
                _logger.debug("File found at %s", path)
                return snapshot_bytes

        raise S3FileNotFoundError(
            f"File {prefix} not found in RCC for extensions {str(extensions)}")

    def _build_chunks_regex(self, metadata_extensions: list[str]) -> re.Pattern:
        """
        Builds a regex to match the chunks of metadata/imu files which have the
            video file name as prefix.

        Args:
            metadata_extensions (list[str]): The metadta

        Raises:
            ValueError: If some of the extensions does not start with a dot (.)

        Returns:
            re.Pattern: A regex pattern
        """
        # Build the regex
        extension_regex = ""
        for chunk_extension in metadata_extensions:
            if not chunk_extension.startswith("."):
                raise ValueError(
                    f"The file extension passed ({chunk_extension}) has to start with a dot.")
            extension_regex += f"\\{chunk_extension}|"

        extension_regex = extension_regex[:-1]

        # Regex to match metadata
        # e.g for metadata r".+\.mp4.*(\.json|\.zip)$"
        return re.compile(rf".+\_(\d+).mp4.+({extension_regex})$")

    def _upload_file(self, upload_path: str, video_bytes: bytes, bucket: Optional[str] = None):
        """Uploads a file to DevCloud S3"""
        if bucket is None:
            bucket = self._container_svcs.raw_s3
        try:
            self._s3_controller.upload_file(video_bytes,
                                            bucket, upload_path)
        except ClientError as exception:
            if self._s3_controller.check_s3_file_exists(bucket, upload_path):
                _logger.info("File %s already exists in %s", upload_path, bucket)
            else:
                _logger.exception("File %s could not be uploaded to DevCloud S3 into %s", upload_path, bucket)
                raise S3UploadError from exception
        return f"s3://{bucket}/{upload_path}"

    @property
    def _rcc_s3_client(self):
        """ RCC boto3 s3 client """
        client = self._rcc_s3_client_factory()
        return client
