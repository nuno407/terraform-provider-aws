from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.models import ChunkDownloadParams, S3ObjectRCC
from sdretriever.s3_downloader_uploader import S3DownloaderUploader
from kink import inject
from sdretriever.s3_crawler_rcc import S3CrawlerRCC
from typing import Optional
import re
import logging

_logger = logging.getLogger(__file__)


@inject
class RCCChunkDownloader:
    """
    Class responsible for downloading the chunks from RCC
    based on the FootageCompleteDevCloudEvent message.
    """

    def __init__(self, s3_crawler: S3CrawlerRCC, s3_downloader: S3DownloaderUploader):
        self.__s3_crawler = s3_crawler
        self.__suffix: Optional[str] = None
        self.__s3_downloader = s3_downloader

        # to be used by the callback function
        self.__match_pattern = re.compile(
            r"^([^\W_]+_[^\W_]+-[a-z0-9\-]+_\d+)\..*$")

    def __file_match(self, file_path: str) -> Optional[str]:
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
        if self.__suffix is None:
            raise ValueError("Suffix cannot be null while searching for chunks in RCC")

        if not file_path.endswith(self.__suffix):
            return None

        match = re.match(self.__match_pattern, file_path)
        if not match:
            return None

        return match.group(1)

    def download_files(self, params: ChunkDownloadParams) -> list[S3ObjectRCC]:
        """
        Download files from RCC based on the FootageCompleteDevCloudEvent message.
        Files that end wihth the .zip extension will also be extracted.
        This will search all the chunks in RCC based on the params and the suffix provided in the constructor.

        Will download the chunks that matches the following conditions:
        - Upload time is between params.start_search and params.stop_search.
        - Matches the filename to the following convention: <recorder>_<recording_id>_<chunk_id>.*<suffix>
        - Filenames are unique based on the chunk_id

        Args:
            params (ChunkDownloadParams): The parameters

        Raises:
            UploadNotYetCompletedError: If not all files can be ingested

        Returns:
            list[S3ObjectRCC]: A list with all the files downloaded.
        """
        self.__suffix = params.suffix
        list_prefix_files_download = set(params.get_chunks_prefix())

        _logger.info("Searching with prefix=%s and suffix=%s",
                     params.get_chunks_prefix(), self.__suffix)
        search_results = self.__s3_crawler.search_files(
            list_prefix_files_download,
            params.get_search_parameters(),
            self.__file_match)

        if len(search_results) != len(list_prefix_files_download):
            raise UploadNotYetCompletedError(
                f"Not all metadata chunks were found {len(search_results)}/{len(search_results)}")

        list_file_path_to_download = set(map(lambda x: x[1].key, search_results.items()))

        return self.__s3_downloader.download_from_rcc(list_file_path_to_download)
