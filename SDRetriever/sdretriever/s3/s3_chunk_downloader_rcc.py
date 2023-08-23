from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.models import ChunkDownloadParamsByID, S3ObjectRCC, RCCS3SearchParams, ChunkDownloadParamsByPrefix
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from kink import inject
from base.aws.model import S3ObjectInfo
from sdretriever.s3.s3_crawler_rcc import S3CrawlerRCC
from typing import Optional
import re
import logging

_logger = logging.getLogger("SDRetriever." + __name__)


@inject
class RCCChunkDownloader:
    """
    Class responsible for downloading the chunks from RCC
    based on the FootageCompleteDevCloudEvent message.

    An object of this class is NOT thread safe.
    """

    def __init__(self, s3_crawler: S3CrawlerRCC, s3_downloader: S3DownloaderUploader):
        self.__s3_crawler = s3_crawler
        self.__suffixes: list[str] = []
        self.__s3_downloader = s3_downloader

        # to be used by the callback function
        self.__match_pattern = re.compile(
            r"^([^\W_]+_[^\W_]+-[a-z0-9\-]+_\d+)\..*$")

    def __file_match(self, obj_info: S3ObjectInfo) -> Optional[str]:
        """
        Callback function to be used on the S3Crawler, it will return
        the file IDs if the suffix match.

        Example 1:
        suffix -> imu_raw.csv.zip
        file_path -> TrainingRecorder_TrainingRecorder-beec7df1-b564-4bb3-9ec7-b83c182491b8_2.mp4._stream1_20230710122027_1_imu_raw.csv.zip
        return -> TrainingRecorder_TrainingRecorder-beec7df1-b564-4bb3-9ec7-b83c182491b8_2

        Example 2:
        suffix -> imu_raw.csv.zip
        file_path -> TrainingRecorder_TrainingRecorder-beec7df1-b564-4bb3-9ec7-b83c182491b8_2.mp4
        return -> None


        Args:
            file_path (str): Name of the file in RCC

        Returns:
            Optional[str]: Return the file ID or None if cannot be extrapolated.
        """
        file_name = obj_info.get_file_name()

        contains_suffix = any(map(lambda suffix: file_name.endswith(suffix), self.__suffixes))
        if not contains_suffix:
            return None

        match = re.match(self.__match_pattern, file_name)
        if not match:
            return None

        return match.group(1)

    def download_by_prefix_suffix(self, params: ChunkDownloadParamsByPrefix) -> list[S3ObjectRCC]:
        """
        Download files from RCC based on the message announced by the device.
        Files that end wihth the .zip extension will also be extracted.
        This will search all the chunks in RCC based on the params and the suffix provided.
        It is important that the files_prefix matches the following convention (<recorder>_<recording_id>_<chunk_id>.<extension>)

        Will download the chunks that matches the following conditions:
        - Matches the filename to the following convention: <recorder>_<recording_id>_<chunk_id>.*<suffix>
        - Upload time is between params.start_search and params.stop_search.
        - Each prefix is unique
        - Only files with one the corresponding prefix and suffix will be downloaded

        Args:
            params (ChunkDownloadParams): The parameters

        Raises:
            UploadNotYetCompletedError: If not all files can be ingested

        Returns:
            list[S3ObjectRCC]: A list with all the files downloaded.
        """
        self.__suffixes = params.suffixes

        # Ensure the files names will be matched by the __file_match method
        list_prefix_files_download = [prefix.split(".")[0] for prefix in params.files_prefix]

        _logger.info("Searching with suffixes=%s",str(self.__suffixes))
        search_results = self.__s3_crawler.search_files(
            list_prefix_files_download,
            params.get_search_parameters(),
            self.__file_match)

        if len(search_results) != len(list_prefix_files_download):
            raise UploadNotYetCompletedError(
                f"Not all metadata chunks were found {len(search_results)}/{len(list_prefix_files_download)}")

        list_file_path_to_download = list(map(lambda x: x[1].key,sorted(search_results.items(), key= lambda x: x[0])))

        return self.__s3_downloader.download_from_rcc(list_file_path_to_download)


    def download_by_chunk_id(self, params: ChunkDownloadParamsByID) -> list[S3ObjectRCC]:
        """
        Download files from RCC based on the FootageCompleteDevCloudEvent message.
        Files that end wihth the .zip extension will also be extracted.
        This will search all the chunks in RCC based on the params and the suffix provided.

        Will download the chunks that matches the following conditions:
        - Upload time is between params.start_search and params.stop_search.
        - Matches the filename to the following convention: <recorder>_<recording_id>_<chunk_id>.*<suffix>
        - Filenames are unique based on the chunk_id

        Args:
            params (ChunkDownloadParams): The parameters

        Raises:
            UploadNotYetCompletedError: If not all files can be ingested

        Returns:
            list[S3ObjectRCC]: A list with all the files downloaded sorted by chunk id.
        """
        self.__suffixes = params.suffixes
        list_prefix_files_download = set(params.get_chunks_prefix())

        _logger.info("Searching with suffixes=%s",str(self.__suffixes))
        search_results = self.__s3_crawler.search_files(
            list_prefix_files_download,
            params.get_search_parameters(),
            self.__file_match)

        if len(search_results) != len(list_prefix_files_download):
            raise UploadNotYetCompletedError(
                f"Not all metadata chunks were found {len(search_results)}/{len(list_prefix_files_download)}")

        # Get the chunks s3 keys and ensure sorted by id
        list_file_path_to_download = list(map(lambda x: x[1].key,sorted(search_results.items(), key= lambda x: x[0])))

        return self.__s3_downloader.download_from_rcc(list_file_path_to_download)
