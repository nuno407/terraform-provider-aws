from unittest.mock import Mock, call, ANY
from itertools import chain

import re
from botocore.errorfactory import ClientError
from sdretriever.exceptions import S3DownloadError
from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture

from base.model.artifacts import (Resolution, S3VideoArtifact, SnapshotArtifact)
from sdretriever.ingestor.s3_video import S3VideoIngestor
from sdretriever.ingestor.post_processor import FFProbeExtractorPostProcessor, VideoInfo
from sdretriever.models import S3ObjectRCC, ChunkDownloadParamsByID, S3ObjectDevcloud
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader


class TestVideoIngestor:

    @fixture()
    def video_ingestor(
            self,
            ffprobe_post_processor: FFProbeExtractorPostProcessor,
            s3_downloader_uploader: S3DownloaderUploader) -> S3VideoIngestor:
        return S3VideoIngestor(
            ffprobe_post_processor,
            s3_downloader_uploader
        )

    @mark.unit()
    @mark.parametrize("artifact", [
        (lazy_fixture("snapshot_artifact")),
        (lazy_fixture("preview_metadata_artifact"))
    ], ids=["fail_snapshot_artifact", "fail_preview_video_artifact"])
    def test_other_artifacts_raise_error(self, artifact: SnapshotArtifact,
                                         video_ingestor: S3VideoArtifact):
        with raises(ValueError):
            video_ingestor.ingest(artifact)

    @mark.unit()
    @mark.parametrize("exception", [
        (ClientError({}, "error")),
        (KeyError(0)),
    ])
    def test_download_failed(
            self,
            exception: Exception,
            video_ingestor: S3VideoIngestor,
            interior_video_artifact: S3VideoArtifact,
            s3_downloader_uploader: S3DownloaderUploader):
        s3_downloader_uploader.download_from_rcc = Mock()
        s3_downloader_uploader.download_from_rcc.side_effect = exception

        with raises(S3DownloadError):
            video_ingestor.ingest(interior_video_artifact)

    @mark.unit()
    @mark.parametrize("artifact,bucket,key", [
        (
            lazy_fixture("interior_video_artifact"),
            lazy_fixture("rcc_bucket"),
            "video.mp4"
        ),
        (
            lazy_fixture("training_video_artifact"),
            lazy_fixture("rcc_bucket"),
            "video.mp4"
        )
    ], ids=["ingest_interior_recorder", "training_interior_recorder"])
    def test_successful_ingestion(
            self,
            artifact: S3VideoArtifact,
            bucket: str,
            key: str,
            video_ingestor: S3VideoIngestor,
            ffprobe_post_processor: FFProbeExtractorPostProcessor,
            s3_downloader_uploader: S3DownloaderUploader):

        # GIVEN
        upload_path = f"s3://{artifact.tenant_id}/{artifact.artifact_id}.mp4"
        video_data = b"mock_data"
        rcc_object = S3ObjectRCC(data=video_data, s3_key=key, bucket=bucket)
        video_info = VideoInfo(1.0, 1920, 1080)

        s3_downloader_uploader.download_from_rcc = Mock(return_value=[rcc_object])
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=upload_path)
        ffprobe_post_processor.execute = Mock(return_value=video_info)

        expected_devcloud_object = S3ObjectDevcloud(
            data=video_data,
            filename=f"{artifact.artifact_id}.mp4",
            tenant=artifact.tenant_id)
        expected_resolution = Resolution(width=video_info.width, height=video_info.height)

        # WHEN
        video_ingestor.ingest(artifact)

        # THEN
        s3_downloader_uploader.download_from_rcc.assert_called_once_with([key], bucket)
        ffprobe_post_processor.execute.assert_called_once_with(video_data)
        s3_downloader_uploader.upload_to_devcloud_raw.assert_called_once_with(
            expected_devcloud_object)
        assert artifact.s3_path == upload_path
        assert artifact.resolution == expected_resolution
        assert artifact.actual_duration == video_info.duration

    @mark.unit
    def test_is_already_ingested(
            self,
            preview_metadata_artifact: S3VideoArtifact,
            video_ingestor: S3VideoIngestor,
            s3_downloader_uploader: S3DownloaderUploader):
        # GIVEN
        is_file_return = Mock()
        s3_downloader_uploader.is_file_devcloud_raw = Mock(return_value=is_file_return)

        # WHEN
        result = video_ingestor.is_already_ingested(preview_metadata_artifact)

        # THEN
        assert result == is_file_return
        s3_downloader_uploader.is_file_devcloud_raw.assert_called_once_with(
            preview_metadata_artifact.artifact_id + ".mp4", preview_metadata_artifact.tenant_id)
