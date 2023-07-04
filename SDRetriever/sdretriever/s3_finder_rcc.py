"""S3 Finder module."""

import logging as log
import re
from calendar import monthrange
from datetime import datetime
from typing import Iterator

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class S3FinderRCC:  # pylint: disable=too-few-public-methods
    """ S3Finder class. """

    def __init__(self, rcc_bucket: str, rcc_s3_client_factory: S3ClientFactory,
                 container_services: ContainerServices):
        """
        Creates a an RCC Finder S3

        Args:
            rcc_bucket (str): The RCC bucket
            rcc_s3_client_factory (S3ClientFactory): The RCC Controler
            container_services (ContainerServices): Container services
        """
        self.__s3_client_fact = rcc_s3_client_factory
        self.__s3_bucket = rcc_bucket
        self.__container_services = container_services

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
        year_groups = re.search(r"year=(\d{4})", path)
        month_groups = re.search(r"month=(\d{2})", path)
        day_groups = re.search(r"day=(\d{2})", path)
        hour_groups = re.search(r"hour=(\d{2})", path)

        # Cast the dates of the paths if they exist
        year = int(year_groups.groups()[0])  # type: ignore
        month = int(month_groups.groups()[
                    0]) if month_groups is not None else None

        day = int(day_groups.groups()[0]
                  ) if day_groups is not None else None

        hour = int(hour_groups.groups()[
            0]) if hour_groups is not None else None

        # kwargs use to replace date
        kwargs_replace = {}

        if year:
            kwargs_replace["year"] = year

        if month:
            kwargs_replace["month"] = month

        if day:
            kwargs_replace["day"] = day

        if hour:
            kwargs_replace["hour"] = hour

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
            path_date_start["day"] = max_day

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
            path_date_end["day"] = max_day

        result_time_end = end_time_zero.replace(
            **path_date_end)  # type: ignore
        result_time_start = start_time_zero.replace(
            **path_date_start)  # type: ignore

        return result_time_start, result_time_end

    def discover_s3_subfolders(  # pylint: disable=too-many-arguments, too-many-locals
            self,
            parent_folder: str,
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
            parent_folder (str): Parent folder to search for. Should be device_id.
            start_time (datetime): Time to start the search
            end_time (datetime): Time to end the search

        Returns:
            Iterator[str]: A list containing all paths from root to the last folder (hour folder).
        """

        _logger.debug(
            "Discovering folders while searching on %s - %s", start_time, end_time)

        # Reset minutes
        start_time_zero = start_time.replace(minute=0)
        end_time_zero = end_time.replace(minute=0)

        rcc_s3_client = self.__s3_client_fact()

        list_objects_response = self.__container_services.list_s3_objects(
            parent_folder, self.__s3_bucket, rcc_s3_client, "/")

        sub_folders = []
        for content in list_objects_response["CommonPrefixes"]:
            path = content["Prefix"]
            current_time_start, current_time_end = self.__get_timestamps_bounds(
                start_time_zero, end_time_zero, path)

            # Make sure the paths are within boundaries
            if current_time_start < start_time_zero or current_time_end > end_time_zero:
                continue

            # Additional check to avoid infinite loop
            if not path.endswith("/"):
                continue

            sub_folders.append(path)

        # Check if this is the latest stopping point
        if "day=" in parent_folder:
            # Make sure all paths are completed and don't have missing folders
            # return [result for result in sub_folders if "hour=" in result]

            for result in sub_folders:
                if "hour=" in result:
                    yield result

            return

        # Call own function again for every result
        for folder in sub_folders:
            yield from self.discover_s3_subfolders(
                folder, start_time, end_time)
