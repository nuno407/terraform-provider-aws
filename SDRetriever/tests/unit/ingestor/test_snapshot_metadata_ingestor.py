from datetime import datetime
from pytz import UTC
from unittest.mock import Mock, call, ANY

from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture

from base.model.artifacts import (PreviewSignalsArtifact, Artifact, SignalsArtifact)
from sdretriever.ingestor.snapshot_metadata import SnapshotMetadataIngestor
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.exceptions import EmptyFileError
from sdretriever.models import ChunkDownloadParamsByPrefix, S3ObjectDevcloud
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader


class TestSnapshotMetadataIngestor:

    @fixture()
    def snapshot_metadata_ingestor(
            self,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader) -> SnapshotMetadataIngestor:
        return SnapshotMetadataIngestor(
            rcc_chunk_downloader,
            s3_downloader_uploader,
        )

    @mark.unit()
    @mark.parametrize("artifact",
                      [(lazy_fixture("snapshot_artifact")),
                       (lazy_fixture("preview_metadata_artifact")),
                          (lazy_fixture("interior_video_artifact")),
                          (lazy_fixture("training_video_artifact"))],
                      ids=["fail_snapshot_artifact",
                           "fail_preview_metadata_artifact",
                           "fail_interior_video_artifact",
                           "fail_training_video_artifact"])
    def test_other_artifacts_raise_error(self, artifact: Artifact,
                                         snapshot_metadata_ingestor: SnapshotMetadataIngestor):
        with raises(ValueError):
            snapshot_metadata_ingestor.ingest(artifact)

    @mark.unit()
    def test_ingest(
            self,
            snapshot_metadata_ingestor: SnapshotMetadataIngestor,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader,
            snapshot_metadata_artifact: SignalsArtifact):

        # GIVEN
        snapshot_name = snapshot_metadata_artifact.artifact_id + ".json"
        path_uploaded = f"s3://bucket/dummy_value"

        downloaded_data = Mock(spec=S3ObjectDevcloud)
        downloaded_data.data = "dummy_data"

        rcc_chunk_downloader.download_by_prefix_suffix = Mock(return_value=[downloaded_data])
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=path_uploaded)

        expected_devcloud_obj = S3ObjectDevcloud(data=downloaded_data.data,
                                                 filename=snapshot_name,
                                                 tenant=snapshot_metadata_artifact.tenant_id)
        reference_current_time = datetime.now(tz=UTC)

        expected_search_params = ChunkDownloadParamsByPrefix(
            device_id=snapshot_metadata_artifact.device_id,
            tenant=snapshot_metadata_artifact.tenant_id,
            start_search=snapshot_metadata_artifact.referred_artifact.timestamp,
            stop_search=ANY,
            suffixes=[
                ".json.zip",
                ".json"],
            files_prefix=[snapshot_metadata_artifact.referred_artifact.uuid])

        # WHEN
        snapshot_metadata_ingestor.ingest(snapshot_metadata_artifact)

        # THEN
        rcc_chunk_downloader.download_by_prefix_suffix.assert_called_once_with(
            params=expected_search_params)

        # Assert end date to search
        rcc_chunk_downloader.download_by_prefix_suffix.call_args[1]["params"].stop_search > reference_current_time

        s3_downloader_uploader.upload_to_devcloud_raw.assert_called_once_with(expected_devcloud_obj)
        assert snapshot_metadata_artifact.s3_path == path_uploaded

    @mark.unit
    def test_is_already_ingested(self,
                                 snapshot_metadata_ingestor: SnapshotMetadataIngestor,
                                 s3_downloader_uploader: S3DownloaderUploader,
                                 snapshot_metadata_artifact: SignalsArtifact):
        # GIVEN
        is_file_return = Mock()
        s3_downloader_uploader.is_file_devcloud_raw = Mock(return_value=is_file_return)

        # WHEN
        result = snapshot_metadata_ingestor.is_already_ingested(snapshot_metadata_artifact)

        # THEN
        assert result == is_file_return
        s3_downloader_uploader.is_file_devcloud_raw.assert_called_once_with(
            snapshot_metadata_artifact.artifact_id + ".json", snapshot_metadata_artifact.tenant_id)

    @mark.unit()
    def test_ingest_empty_metadata(
            self,
            snapshot_metadata_ingestor: SnapshotMetadataIngestor,
            rcc_chunk_downloader: RCCChunkDownloader,
            s3_downloader_uploader: S3DownloaderUploader,
            snapshot_metadata_artifact: SignalsArtifact):

        # GIVEN
        path_uploaded = f"s3://bucket/dummy_value"

        downloaded_data = Mock(spec=S3ObjectDevcloud)
        downloaded_data.data = b""

        rcc_chunk_downloader.download_by_prefix_suffix = Mock(return_value=[downloaded_data])
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=path_uploaded)

        # WHEN
        with raises(EmptyFileError):
            snapshot_metadata_ingestor.ingest(snapshot_metadata_artifact)
