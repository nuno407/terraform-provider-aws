""" ingestor module. """
import logging as log
import re
from abc import abstractmethod
from calendar import monthrange
from datetime import datetime
from typing import Iterator, Optional, Tuple

import boto3
from mypy_boto3_s3 import S3Client
from base.aws.container_services import ContainerServices, RCCS3ObjectParams
from base.aws.shared_functions import StsHelper


LOGGER = log.getLogger("SDRetriever." + __name__)


class Ingestor():
    """Ingestor base class"""

    def __init__(
            self,
            container_services: ContainerServices,
            s3_client: S3Client,
            sqs_client,
            sts_helper: StsHelper) -> None:
        """_summary_

        Args:
            container_services (ContainerServices): instance of ContainerServices
            s3_client (S3Client): _instance of S3Client
            sqs_client (boto3 SQS client): instance of boto3 SQS client
            sts_helper (StsHelper): instance of StsHelper
        """
        self.container_svcs = container_services
        self.s3_client = s3_client
        self.sqs_client = sqs_client
        self.sts_helper = sts_helper
        self._rcc_credentials = self.sts_helper.get_credentials()
        self.metadata_queue = self.container_svcs.sqs_queues_list["Metadata"]
        self.rexp_mp4 = re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")

        # Build regex for mp4 (RecorderType, RecorderType, id)
        self.rexp_mp4 = re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")

    @abstractmethod
    def ingest(self, message):
        """Ingests the artifacts described in a message into the DevCloud"""

    def check_if_s3_rcc_path_exists(self, s3_path: str, bucket: str, messageid: Optional[str] = None,
                                    max_s3_api_calls: int = 1, exact=False) -> Tuple[bool, dict]:
        """Verify if path exists on target S3 bucket.

        - Check if the file exists in a given path.
        - Verifies if the object found size is bigger then 0.
        - If the object is not found, continue to look for tenant and deviceid prefixes to provide logging information
        - If the exact argument is true, all objects will still be retrieved but the first return value will only be true if one of
        the objects matches th path exactly.

        Args:
            s3_path (str): S3 path to look for.
            bucket (str): S3 bucket name.
            messageid (str): SQS message ID
            max_s3_api_calls (str): Maximum s3 list api calls to be made
            exact (bool): Only match files with exact name, if false it will only match the prefix
        Returns:
            A tuple containing a boolean response if the path was found and a dictionary with the S3 objects information
        """
        s3_object_params = RCCS3ObjectParams(s3_path=s3_path, bucket=bucket)
        s3_client = self.RCC_S3_CLIENT

        # Check if there is a file with the same name already stored on target S3 bucket
        try:
            list_objects_response: dict = dict(ContainerServices.list_s3_objects(
                s3_object_params.s3_path, bucket, s3_client, max_iterations=max_s3_api_calls))

            if list_objects_response.get('KeyCount') and int(list_objects_response['Contents'][0]['Size']) == 0:
                return False, list_objects_response

        except Exception:  # pylint: disable=broad-except
            return ContainerServices.check_if_tenant_and_deviceid_exists_and_log_on_error(
                s3_client, s3_object_params, messageid), {}

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
    ) -> Optional[bytearray]:
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
            Optional[bytearray]: Returns the contents of the file if found.
        """
        if extensions is None:
            extensions = []
        subfolders: Iterator[str] = self._discover_s3_subfolders(
            f'{tenant}/{device_id}/', bucket, self.RCC_S3_CLIENT, start_time, end_time)

        for subfolder in subfolders:
            path = subfolder + prefix
            file_found, list_objects_response = self.check_if_s3_rcc_path_exists(path, bucket)

            if not file_found:
                continue

            # Filter the file for any of the extensions provided
            files = list(filter(lambda x: any(x['Key'].endswith(extension)
                                              for extension in extensions),
                                list_objects_response['Contents']))

            if len(files) > 1:
                raise RuntimeError(f"Found more then one file for {path}, files found: {str(files)}")

            if len(files) == 1:
                snapshot_bytes: bytearray = self.container_svcs.download_file(self.RCC_S3_CLIENT, bucket, path)
                LOGGER.debug("File found at %s", path)
                return snapshot_bytes

        raise FileNotFoundError(f"File {prefix} not found in RCC for extensions {str(extensions)}")

    def _build_chunks_regex(self, metadata_extensions: list[str]) -> re.Pattern:
        """
        Builds a regex to match the chunks of metadata/imu files which have the video file name as prefix.

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
                raise ValueError(f"The file extension passed ({chunk_extension}) has to start with a dot.")
            extension_regex += f"\\{chunk_extension}|"

        extension_regex = extension_regex[:-1]

        # Regex to match metadata
        # e.g for metadata r".+\.mp4.*(\.json|\.zip)$"
        return re.compile(rf".+\_(\d+).mp4.+({extension_regex})$")

    def __convert_date_string_path(self, path: str) -> dict:
        """
        Parses the RCC file structure to the sppecific dates to be used by the discover_s3 function.
        Converts an RCC-like path into separate variables:
            "/YYYY/MM/DD/hh/" -> {year:YYYY, month:MM, day:DD, hour:hh}

        Args:
            path (str): RCC s3 path.

        Returns:
            dict: A dictionary containing (year,month,day and hour) if available.
        """
        year_groups = re.search("year=([0-9]{4})", path)
        month_groups = re.search("month=([0-9]{2})", path)
        day_groups = re.search("day=([0-9]{2})", path)
        hour_groups = re.search("hour=([0-9]{2})", path)

        # Cast the dates of the paths if they exist
        year = int(year_groups.groups()[0])
        month = int(month_groups.groups()[
                    0]) if month_groups is not None else None

        day = int(day_groups.groups()[0]
                  ) if day_groups is not None else None

        hour = int(hour_groups.groups()[
            0]) if hour_groups is not None else None

        # kwargs use to replace date
        kwargs_replace = {}

        if year:
            kwargs_replace['year'] = year

        if month:
            kwargs_replace['month'] = month

        if day:
            kwargs_replace['day'] = day

        if hour:
            kwargs_replace['hour'] = hour

        return kwargs_replace

    def __get_timestamps_bounds(self, start_time_zero: datetime, end_time_zero: datetime,
                                path: str) -> tuple[datetime, datetime]:
        """
        Get the timestamp bounds for the discover_s3 function.
        Calculates the two timestamps for beginning and end of a comparison.
        Ex:
            /2023/02/14/ => /2023/02/14/00/ -> /2023/02/14/23/
            /2023/02/ => /2023/02/1/00/ -> /2023/02/28/23/
            /2023/ => /2023/01/1/00/ -> /2023/12/31/23/


        Args:
            start_time_zero (datetime): Start datetime
            end_time_zero (datetime): End datetime
            path (str): Path of the rcc path

        Returns:
            tuple[datetime, datetime]: Datetime
        """
        path_date = self.__convert_date_string_path(path)

        path_date_start = {
            "year": start_time_zero.year,
            "month": start_time_zero.month,
            "day": start_time_zero.day,
            "hour": start_time_zero.hour
        }
        path_date_start.update(path_date)

        # Make sure the month is valid for start date
        max_day = monthrange(
            path_date_start["year"], path_date_start["month"])[1]
        if max_day < path_date_start["day"]:
            path_date_start['day'] = max_day

        path_date_end = {
            "year": end_time_zero.year,
            "month": end_time_zero.month,
            "day": end_time_zero.day,
            "hour": end_time_zero.hour
        }
        path_date_end.update(path_date)

        # Make sure the month is valid for end date
        max_day = monthrange(
            path_date_end["year"], path_date_end["month"])[1]
        if max_day < path_date_end["day"]:
            path_date_end['day'] = max_day

        result_time_end = end_time_zero.replace(
            **path_date_end)
        result_time_start = start_time_zero.replace(
            **path_date_start)

        return result_time_start, result_time_end

    def _discover_s3_subfolders(
            self,
            parent_folder: str,
            bucket: str,
            s3_client: S3Client,
            start_time: datetime,
            end_time: datetime) -> Iterator[str]:
        """
        Recursive function that will discover all RCC S3 subfolder between start_time and end_time.
        Return an Iterator over all the prefixes available in S3 between start_time and end_time,
        including the start hour and end hour folder.
        Ex:
            bucket, start_time, end_time =>
                bucket/earliest_path/, bucket/time_1, ..., bucket/latest_time
            (with start_time <= earliest_time <= latest_time <= end_time)

        Args:
            folder (str): Parent folder to search for. Needs to be inside a device folder.
            bucket (str): Bucket name.
            s3_client (S3Client): RCC S3 client.
            start_time (datetime): Time to start the search
            end_time (datetime): Time to end the search

        Returns:
            Iterator[str]: A list containing all paths from root to the last folder (hour folder).
        """

        LOGGER.debug("Discovering folders while searching on %s - %s", start_time, end_time)

        # Reset minutes
        start_time_zero = start_time.replace(minute=0)
        end_time_zero = end_time.replace(minute=0)

        list_objects_response = ContainerServices.list_s3_objects(
            parent_folder, bucket, s3_client, "/")

        sub_folders = []
        for content in list_objects_response['CommonPrefixes']:
            path = content['Prefix']
            current_time_start, current_time_end = self.__get_timestamps_bounds(
                start_time_zero, end_time_zero, path)

            # Make sure the paths are within boundaries
            if current_time_start < start_time_zero or current_time_end > end_time_zero:
                continue

            # Additional check to avoid infinite loop
            if not path.endswith('/'):
                continue

            sub_folders.append(path)

        # Check if this is the latest stopping point
        if 'day=' in parent_folder:
            # Make sure all paths are completed and don't have missing folders
            # return [result for result in sub_folders if 'hour=' in result]

            for result in sub_folders:
                if 'hour=' in result:
                    yield result

            return

        # Call own function again for every result
        for folder in sub_folders:
            yield from self._discover_s3_subfolders(
                folder, bucket, s3_client, start_time, end_time)

    @ property
    def RCC_S3_CLIENT(self):
        """ RCC boto3 s3 client """
        client = boto3.client('s3',
                              region_name='eu-central-1',
                              aws_access_key_id=self.accesskeyid,
                              aws_secret_access_key=self.secretaccesskey,
                              aws_session_token=self.sessiontoken)
        return client

    @ property
    def accesskeyid(self):
        self._rcc_credentials = self.sts_helper.get_credentials()
        return self._rcc_credentials['AccessKeyId']

    @ property
    def secretaccesskey(self):
        self._rcc_credentials = self.sts_helper.get_credentials()
        return self._rcc_credentials['SecretAccessKey']

    @ property
    def sessiontoken(self):
        self._rcc_credentials = self.sts_helper.get_credentials()
        return self._rcc_credentials['SessionToken']
