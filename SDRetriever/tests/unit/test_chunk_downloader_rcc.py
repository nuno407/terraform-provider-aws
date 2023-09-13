# type: ignore
""" Test S3 finder """
from datetime import datetime
from unittest.mock import Mock, ANY
import pytest
from base.model.artifacts import RecorderType
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.models import ChunkDownloadParamsByID, ChunkDownloadParamsByPrefix, RCCS3SearchParams


DUMMY_DATE_START = datetime(year=2023, month=2, day=20, hour=10, minute=10)
DUMMY_DATE_END = datetime(year=2023, month=2, day=20, hour=10, minute=20)


class TestRCCChunkDownloader:

    @pytest.fixture
    def s3_chunk_downloader(self, s3_crawler: S3CrawlerRCC,
                            s3_downloader_uploader: S3DownloaderUploader) -> RCCChunkDownloader:
        return RCCChunkDownloader(s3_crawler, s3_downloader_uploader)

    @pytest.mark.unit
    def test_download_by_prefix_suffix_fail(self,
                                            s3_chunk_downloader: RCCChunkDownloader,
                                            s3_crawler: S3CrawlerRCC):
        """
        Test that UploadNotYetCompletedError is raised if search doesn't work
        """
        # GIVEN
        prefix = "dummy_prefix"
        list_prefix = [prefix, f"{prefix}.test", "test_123"]
        expected_search_prefix = set([prefix, "test_123"])
        search_result = {prefix: Mock()}
        params = ChunkDownloadParamsByPrefix(
            device_id="test_device",
            tenant="test_tenant",
            start_search=DUMMY_DATE_START,
            stop_search=DUMMY_DATE_END,
            files_prefix=list_prefix,
            suffixes=[".mp4"]
        )
        s3_crawler.search_files = Mock(return_value=search_result)

        # WHEN
        with pytest.raises(UploadNotYetCompletedError):
            s3_chunk_downloader.download_by_prefix_suffix(params)

        # THEN
        s3_crawler.search_files.assert_called_once_with(
            expected_search_prefix, params.get_search_parameters(), ANY)

    @pytest.mark.unit
    def test_download_by_chunk_id_fail(self,
                                       s3_chunk_downloader: RCCChunkDownloader,
                                       s3_crawler: S3CrawlerRCC):
        """
        Test that UploadNotYetCompletedError is raised if search doesn't work
        """
        # GIVEN
        prefix = "dummy_prefix"
        search_result = {prefix: Mock()}
        params = ChunkDownloadParamsByID(
            recorder=RecorderType.INTERIOR,
            recording_id="recording_id",
            chunk_ids=[1, 2, 3],
            device_id="test_device",
            tenant="test_tenant",
            start_search=DUMMY_DATE_START,
            stop_search=DUMMY_DATE_END,
            suffixes=[".mp4"]
        )
        s3_crawler.search_files = Mock(return_value=search_result)

        # WHEN
        with pytest.raises(UploadNotYetCompletedError):
            s3_chunk_downloader.download_by_chunk_id(params)

        # THEN
        s3_crawler.search_files.assert_called_once_with(
            set(params.get_chunks_prefix()), params.get_search_parameters(), ANY)

    @pytest.mark.unit
    def download_by_file_name_fail(self,
                                   s3_chunk_downloader: RCCChunkDownloader,
                                   s3_crawler: S3CrawlerRCC):
        """
        Test that UploadNotYetCompletedError is raised if search doesn't work
        """
        # GIVEN
        file_names = ["file_1", "file_2", "file_2"]
        search_result = {"test_mock": Mock()}
        params = RCCS3SearchParams(
            device_id="test_device",
            tenant="test_tenant",
            start_search=DUMMY_DATE_START,
            stop_search=DUMMY_DATE_END
        )
        s3_crawler.search_files = Mock(return_value=search_result)

        # WHEN
        with pytest.raises(UploadNotYetCompletedError):
            s3_chunk_downloader.download_by_chunk_id(params)

        # THEN
        s3_crawler.search_files.assert_called_once_with(set(file_names), params)
