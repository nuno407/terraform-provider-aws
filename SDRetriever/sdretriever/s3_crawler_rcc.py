"""S3 Crawler"""
import logging
import os
from typing import Callable, Iterator, Optional

from kink import inject

from base.aws.model import S3ObjectInfo
from base.aws.s3 import S3Controller, S3ControllerFactory
from sdretriever.models import RCCS3SearchParams
from sdretriever.s3_finder_rcc import S3FinderRCC

_logger = logging.getLogger("SDRetriever." + __name__)


@inject
class S3CrawlerRCC():
    """
    A Crawler to handle the file structure of RCC in the rcc-device-data bucket.
    """

    def __init__(
            self,
            rcc_s3_controller: S3ControllerFactory,
            rcc_bucket: str,
            rcc_s3_finder: S3FinderRCC):
        self.__s3_controller_fact: S3ControllerFactory = rcc_s3_controller
        self.__s3_finder: S3FinderRCC = rcc_s3_finder
        self.__s3_bucket: str = rcc_bucket

    def list_all_objects(self, rcc_s3_params: RCCS3SearchParams,
                         prefix: str = "") -> Iterator[S3ObjectInfo]:
        """
        Returns ALL objects information in RCC from a particular device in between 2 utc timestamps.
        Is possible to provide a prefix in order to speedup the search and decrease the number of API calls.

        REMARKS:
        This will list all objects from S3 bypassing the 1000 object limitation.
        The time bounds for the search are only applied on the folder path! This means
            that some objects returned migh have been uploaded up to 1 hour in the past and 1 hour in the future.
        Args:
            tenant (str): The tenant to search on.
            device_id (str): The device ID to search on.
            start_search (datetime): The UTC timestamp from when to start searching the bucket.
            stop_search (datetime, optional): The UTC timestamp of where the search should finish.
                                              Defaults to datetime.now().
            prefix (str, optional): A file prefix to be appended when listing the folders,
                                    this should be passed whenever possible
                                    to decrease the number of API calls. Defaults to "".

        Yields:
            Iterator[S3ObjectInfo]: An iterator with the object information.
        """
        _logger.debug("Searching for tenant=(%s),device=(%s),from=(%s),to=(%s)", rcc_s3_params.tenant,
                      rcc_s3_params.device_id, rcc_s3_params.start_search, rcc_s3_params.stop_search)

        parent_folder = f"{rcc_s3_params.tenant}/{rcc_s3_params.device_id}/"

        folders: Iterator[str] = self.__s3_finder.discover_s3_subfolders(
            parent_folder, rcc_s3_params.start_search, rcc_s3_params.stop_search)

        s3_controller: S3Controller = self.__s3_controller_fact()

        for folder in folders:
            prefixed_folder = os.path.join(folder, prefix)
            yield from s3_controller.list_directory_objects(prefixed_folder, self.__s3_bucket)

    def search_files(self,
                     files: set[str],
                     rcc_s3_params: RCCS3SearchParams,
                     match_to_file: Callable[[S3ObjectInfo],
                                             Optional[str]] = lambda obj: obj.get_file_name()) -> dict[str,
                                                                                                       S3ObjectInfo]:
        """
        Searches for multiple files inside the RCC rcc-device-data bucket from a particular device
        in between 2 utc timestamps.
        Once one match have been found for every file, the search will stop.

        This also means that in case of multiple matches for
        one of the files, only the first one will be returned.

        The match_to_file parameter should be used to parse the S3ObjectInfo
        and return one of the files in "files", or None, if it is not a mtach.

        In short, the function exits when match_to_file returns all elements contained in the set "files".
        To search for a exact file names, match_to_file does not need to be passed.

        REMARKS:
        This will list all objects from S3 bypassing the 1000 object limitation.

        The time bounds for the search are only applied on the folder path! This means
        that some objects returned migh have been uploaded up to 1 hour in the past and 1 hour in the future.

        A common prefix will be generated from all the files to attempt to reduce the number of S3 API calls.

        Args:
            tenant (str): The tenant to search on.
            files (set[str]): The files to search for in RCC.
            device_id (str): The device ID to search on.
            start_search (datetime): The UTC timestamp from when to start searching the bucket.
            stop_search (datetime, optional): The UTC timestamp of where the search should finish.
                                                Defaults to datetime.now().
            match_to_file (str, Callable[[S3ObjectInfo], str]): A function that will take an S3ObjectInfo and
                                returns a string that is checked against the files to find a match.

        Returns:
            dict[str, S3ObjectInfo]: A dictionary containing the file names and the matches found.
        """

        load_counter = 0

        common_prefix = os.path.commonprefix(list(files))
        object_keys: Iterator[S3ObjectInfo] = self.list_all_objects(rcc_s3_params, common_prefix)

        files_stack = files.copy()
        result: dict[str, S3ObjectInfo] = {}
        for object_key in object_keys:

            if len(files_stack) == 0:
                break

            load_counter += 1

            part_to_match = match_to_file(object_key)
            if part_to_match and part_to_match in files:
                result[part_to_match] = object_key
                files_stack.remove(part_to_match)

        _logger.info(
            "A total of %d files were searched in RCC and %d matches were found out of %d",
            load_counter,
            len(result),
            len(files))

        return result
