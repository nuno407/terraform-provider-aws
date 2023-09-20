# mypy: disable-error-code=attr-defined
"""Unit tests for the SnapshotIngestor class."""
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock

from pytest import fixture, mark, raises
from pytz import UTC

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (RecorderType, Recording, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from sdretriever.config import SDRetrieverConfig
from base.model.artifacts import Artifact
from sdretriever.ingestor.snapshot import SnapshotIngestor
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.models import S3ObjectDevcloud, RCCS3SearchParams
from pytest_lazyfixture import lazy_fixture


class TestSnapshotIngestor:

    @fixture()
    def snapshot_ingestor(
            self,
            s3_downloader_uploader: S3DownloaderUploader,
            rcc_chunk_downloader: RCCChunkDownloader) -> SnapshotIngestor:
        return SnapshotIngestor(
            s3_downloader_uploader,
            rcc_chunk_downloader
        )

    @mark.unit()
    @mark.parametrize("artifact", [
        (lazy_fixture("preview_metadata_artifact")),
        (lazy_fixture("interior_video_artifact")),
        (lazy_fixture("training_video_artifact"))
    ], ids=["fail_preview_artifact", "fail_interior_video_artifact", "fail_training_video_artifact"])
    def test_other_artifacts_raise_error(self, artifact: Artifact,
                                         snapshot_ingestor: SnapshotIngestor):
        with raises(ValueError):
            snapshot_ingestor.ingest(artifact)

    @mark.unit()
    def test_ingest(
            self,
            snapshot_ingestor: SnapshotIngestor,
            snapshot_artifact: SnapshotArtifact,
            s3_downloader_uploader: S3DownloaderUploader,
            rcc_chunk_downloader: RCCChunkDownloader):

        # GIVEN
        snapshot_name = snapshot_artifact.artifact_id + ".jpeg"
        path_uploaded = f"s3://bucket/dummy_value"

        downloaded_data = Mock()

        rcc_chunk_downloader.download_by_file_name = Mock(return_value=[downloaded_data])
        s3_downloader_uploader.upload_to_devcloud_raw = Mock(return_value=path_uploaded)

        expected_devcloud_obj = S3ObjectDevcloud(data=downloaded_data.data,
                                                 filename=snapshot_name,
                                                 tenant=snapshot_artifact.tenant_id)
        reference_current_time = datetime.now(tz=UTC)
        expected_search_params = RCCS3SearchParams(
            device_id=snapshot_artifact.device_id,
            tenant=snapshot_artifact.tenant_id,
            start_search=snapshot_artifact.timestamp,
            stop_search=ANY)

        # WHEN
        snapshot_ingestor.ingest(snapshot_artifact)

        # THEN
        rcc_chunk_downloader.download_by_file_name.assert_called_once_with(
            file_names=[snapshot_artifact.uuid], search_params=expected_search_params)
        # Assert end date to search
        rcc_chunk_downloader.download_by_file_name.call_args[1]["search_params"].stop_search > reference_current_time

        s3_downloader_uploader.upload_to_devcloud_raw.assert_called_once_with(expected_devcloud_obj)
        assert snapshot_artifact.s3_path == path_uploaded

    @mark.unit
    def test_is_already_ingested(
            self,
            snapshot_ingestor: SnapshotIngestor,
            snapshot_artifact: SnapshotArtifact,
            s3_downloader_uploader: S3DownloaderUploader):
        # GIVEN
        is_file_return = Mock()
        s3_downloader_uploader.is_file_devcloud_raw = Mock(return_value=is_file_return)

        # WHEN
        result = snapshot_ingestor.is_already_ingested(snapshot_artifact)

        # THEN
        assert result == is_file_return
        s3_downloader_uploader.is_file_devcloud_raw.assert_called_once_with(
            snapshot_artifact.artifact_id + ".jpeg", snapshot_artifact.tenant_id)
