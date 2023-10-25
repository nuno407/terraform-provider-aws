"""Test chc container handler"""
import pytest
from datetime import timedelta
from unittest.mock import Mock
from artifact_downloader.post_processor import FFProbeExtractorPostProcessor, VideoInfo
from artifact_downloader.chc_synchronizer import ChcSynchronizer
from artifact_downloader.request_factory import RequestFactory
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.container_handlers.chc import CHCContainerHandler


class TestCHC:

    @pytest.fixture
    def chc_container_handler(
            self,
            mock_post_processor: FFProbeExtractorPostProcessor,
            mock_chc_syncronizer: ChcSynchronizer,
            mock_request_factory: RequestFactory,
            mock_s3_downloader: S3Downloader):
        """Fxiture for chc container handler"""
        return CHCContainerHandler(request_factory=mock_request_factory,
                                   s3_downloader=mock_s3_downloader,
                                   post_processor=mock_post_processor,
                                   chc_syncronizer=mock_chc_syncronizer)

    @pytest.mark.unit
    def test_download_and_synchronize_chc(
            self,
            chc_container_handler: CHCContainerHandler,
            mock_s3_downloader: S3Downloader,
            mock_chc_syncronizer: ChcSynchronizer,
            mock_post_processor: FFProbeExtractorPostProcessor):
        """Test download and syncronize"""
        # GIVEN
        mock_video_s3_path = "s3://bucket/somevideo.mp4"
        mock_chc_s3_path = "s3://bucket/somemetadata.json"

        mock_video = b"video_data"
        mock_chc_data = {"some_data": "test"}
        mock_sync = {timedelta(seconds=0.1): {"some_field": 1.0}}
        mock_probe_info = VideoInfo(duration=50.2, width=1, height=1)

        mock_s3_downloader.download = Mock(return_value=mock_video)  # type: ignore
        mock_post_processor.execute = Mock(return_value=mock_probe_info)  # type: ignore
        mock_s3_downloader.download_convert_json = Mock(return_value=mock_chc_data)  # type: ignore
        mock_chc_syncronizer.synchronize = Mock(return_value=mock_sync)  # type: ignore

        # WHEN
        result = chc_container_handler.download_and_synchronize_chc(mock_chc_s3_path, mock_video_s3_path)

        # THEN
        mock_s3_downloader.download.assert_called_once_with(mock_video_s3_path)
        mock_post_processor.execute.assert_called_once_with(mock_video)
        mock_s3_downloader.download_convert_json.assert_called_once_with(mock_chc_s3_path)
        mock_chc_syncronizer.synchronize.assert_called_once_with(mock_chc_data, timedelta(seconds=50.2))
        assert result == mock_sync
