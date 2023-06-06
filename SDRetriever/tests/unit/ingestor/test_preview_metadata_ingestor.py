import os
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from pytest_lazyfixture import lazy_fixture

from pytest import fixture, mark, raises
from pytz import UTC

from base.timestamps import from_epoch_seconds_or_milliseconds
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (RecorderType, PreviewSignalsArtifact, MultiSnapshotArtifact, VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from sdretriever.exceptions import UploadNotYetCompletedError, S3FileNotFoundError
from sdretriever.ingestor.preview_metadata import PreviewMetadataIngestor
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3_finder_rcc import S3FinderRCC
from sdretriever.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.config import SDRetrieverConfig
from sdretriever.ingestor.metacontent import (MetacontentChunk, MetacontentDevCloud)
from sdretriever.models import RCCS3SearchParams
from base.aws.model import S3ObjectInfo

CONST_TIME = datetime.now(tz=UTC) - timedelta(hours=4)
DUMMY_UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
DUMMY_UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
TENANT_ID = "datanauts"
DEVICE_ID = "rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc"


def helper_convert_timestamp_to_path(timestamp: datetime) -> str:
    return f"year={timestamp.year}/month={timestamp.month}/day={timestamp.day}/hour={timestamp.hour}"


def rcc_crawler_search_result(
        multi_snapshot_artifact: MultiSnapshotArtifact) -> dict[str, S3ObjectInfo]:

    result_return: dict[str, S3ObjectInfo] = {}

    for snap_art in multi_snapshot_artifact.chunks:

        time_path = helper_convert_timestamp_to_path(snap_art.timestamp)
        s3_key = os.path.join(
            "/",
            multi_snapshot_artifact.tenant_id,
            multi_snapshot_artifact.device_id,
            time_path,
            f"{snap_art.uuid}_stream_mock_pose.json"
        )
        s3_info = S3ObjectInfo(
            key=s3_key,
            date_modified=snap_art.upload_timing.end,
            size=0,
        )
        result_return[snap_art.uuid] = s3_info

    return result_return


class TestPreviewMetadataIngestor:

    @fixture()
    def preview_metadata_ingestor(
            self,
            container_services: ContainerServices,
            s3_client_factory: S3ClientFactory,
            s3_controller: S3Controller,
            s3_crawler: S3CrawlerRCC,
            s3_finder: S3FinderRCC,
            config: SDRetrieverConfig,
            metadata_merger: MetadataMerger) -> PreviewMetadataIngestor:
        return PreviewMetadataIngestor(
            container_services,
            s3_client_factory,
            s3_controller,
            s3_crawler,
            s3_finder,
            config,
            metadata_merger
        )

    @fixture()
    def video_artifact(self) -> VideoArtifact:
        """VideoArtifact for testing."""
        return VideoArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            recorder=RecorderType.INTERIOR,
            timestamp=CONST_TIME,
            upload_timing=TimeWindow(
                start=DUMMY_UPLOAD_START,
                end=DUMMY_UPLOAD_END
            ),
            stream_name="baz",
            end_timestamp=CONST_TIME
        )

    @fixture()
    def snapshot_1(self) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
            device_id=DEVICE_ID,
            tenant_id=TENANT_ID,
            timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
            end_timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
            recorder=RecorderType.INTERIOR_PREVIEW,
            upload_timing=TimeWindow(
                start="2023-05-31T14:03:51.613360+00:00",
                end="2023-05-31T15:03:51.613360+00:00"))

    @fixture()
    def snapshot_2(self) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
            device_id=DEVICE_ID,
            tenant_id=TENANT_ID,
            timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
            end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
            recorder=RecorderType.INTERIOR_PREVIEW,
            upload_timing=TimeWindow(
                start="2023-05-31T14:03:51.613360+00:00",
                end="2023-05-31T15:03:51.613360+00:00"))

    @ fixture()
    def multi_snapshot_artifact(
            self,
            snapshot_1: SnapshotArtifact,
            snapshot_2: SnapshotArtifact) -> MultiSnapshotArtifact:
        return MultiSnapshotArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
            end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
            recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
            upload_timing=TimeWindow(
                start="2023-05-31T14:03:51.613360+00:00",
                end="2023-05-31T15:03:51.613360+00:00"),
            recorder=RecorderType.INTERIOR_PREVIEW,
            chunks=[
                snapshot_1,
                snapshot_2])

    @ fixture()
    def preview_metadata_artifact(
            self, multi_snapshot_artifact: MultiSnapshotArtifact) -> PreviewSignalsArtifact:
        return PreviewSignalsArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            referred_artifact=multi_snapshot_artifact,
            timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
            end_timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
        )

    @ mark.unit()
    @ mark.parametrize("artifact", [
        (lazy_fixture("video_artifact")),
        (lazy_fixture("snapshot_1")),
    ])
    def test_other_artifacts_raise_error(self, artifact: SnapshotArtifact,
                                         preview_metadata_ingestor: PreviewMetadataIngestor):
        with raises(ValueError):
            preview_metadata_ingestor.ingest(artifact)

    @ mark.unit()
    def test_raise_if_not_all_parts_exist(self,
                                          preview_metadata_ingestor: PreviewMetadataIngestor,
                                          preview_metadata_artifact: PreviewSignalsArtifact,
                                          s3_crawler: S3CrawlerRCC):
        # GIVEN
        s3_crawler.search_files = Mock(
            return_value={
                "mock": MagicMock()})  # type: ignore[method-assign]

        # WHEN
        with raises(UploadNotYetCompletedError):
            preview_metadata_ingestor.ingest(preview_metadata_artifact)

    @ mark.unit()
    def test_raise_if_not_able_to_download(self,
                                           preview_metadata_ingestor: PreviewMetadataIngestor,
                                           preview_metadata_artifact: PreviewSignalsArtifact,
                                           s3_crawler: S3CrawlerRCC):
        # GIVEN
        s3_crawler.search_files = Mock(
            return_value={
                "mock1": MagicMock(),
                "mock2": MagicMock()})  # type: ignore[method-assign]
        preview_metadata_ingestor._get_metacontent_chunks = Mock(return_value=[Mock()])

        # WHEN
        with raises(S3FileNotFoundError):
            preview_metadata_ingestor.ingest(preview_metadata_artifact)

    @ mark.unit()
    @ mark.parametrize(
        "chunk,expected",
        [
            (S3ObjectInfo(
                key="datanauts/DATANAUTS_TEST_02/year=2023/month=06/day=01/hour=13/InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1.jpeg._stream1_20230601095334_pose.json.zip",
                date_modified=datetime.now(),
                size=0),
                "InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1.jpeg"),
            (S3ObjectInfo(
                key="datanauts/DATANAUTS_TEST_02/year=2023/month=06/day=01/hour=13/InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1.jpeg._stream1_20230601095334_pose.json",
                date_modified=datetime.now(),
                size=0),
                "InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1.jpeg"),
            (S3ObjectInfo(
                key="datanauts/DATANAUTS_TEST_02/year=2023/month=06/day=01/hour=13/InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1.jpeg",
                date_modified=datetime.now(),
                size=0),
                None),
            (S3ObjectInfo(
                key="datanauts/DATANAUTS_TEST_02/year=2023/month=06/day=01/hour=13/InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1",
                date_modified=datetime.now(),
                size=0),
                None)])
    def test_metadata_chunk_match(self, chunk: S3ObjectInfo, expected: str,
                                  preview_metadata_ingestor: PreviewMetadataIngestor):

        assert preview_metadata_ingestor.metadata_chunk_match(chunk) == expected

    @ mark.unit()
    @patch("datetime.now")
    def test_successful_ingestion(
            self,
            mock_dt: Mock,
            config: SDRetrieverConfig,
            preview_metadata_ingestor: PreviewMetadataIngestor,
            preview_metadata_artifact: PreviewSignalsArtifact,
            s3_crawler: S3CrawlerRCC,
            metadata_merger: MetadataMerger):
        # GIVEN
        dt = Mock()
        mock_dt.return_value = dt
        multi_snapshot_artifact = preview_metadata_artifact.referred_artifact
        snapshot_names = {
            snapshot_artifact.uuid for snapshot_artifact in preview_metadata_artifact.referred_artifact.chunks}
        devcloud_path_uploaded = "s3://some/path"
        downloaded_metachunks = [
            MetacontentChunk(
                b"MOCK",
                "mock_path") for _ in multi_snapshot_artifact.chunks]
        metadata_chunks_found = rcc_crawler_search_result(multi_snapshot_artifact)
        metadata_chunks_keys = set(map(lambda x: x.key, metadata_chunks_found.values()))
        merged_chunks = {"Mock_key": "Mock_value"}
        bytes_merged_chunks = bytes(json.dumps(merged_chunks, ensure_ascii=False).encode('UTF-8'))

        rcc_s3_params = RCCS3SearchParams(tenant=preview_metadata_artifact.tenant_id,
                                          device_id=preview_metadata_artifact.device_id,
                                          start_search=preview_metadata_artifact.timestamp,
                                          stop_search=dt)

        devcloud_content = MetacontentDevCloud(
            bytes_merged_chunks,
            preview_metadata_artifact.artifact_id,
            config.temporary_bucket,
            preview_metadata_artifact.tenant_id,
            ".json")

        s3_crawler.search_files = Mock(return_value=metadata_chunks_found)
        preview_metadata_ingestor._get_metacontent_chunks = Mock(return_value=downloaded_metachunks)
        preview_metadata_ingestor._upload_metacontent_to_devcloud = Mock(
            return_value=devcloud_path_uploaded)
        metadata_merger.merge_metadata_chunks = Mock(return_value=merged_chunks)

        # WHEN
        preview_metadata_ingestor.ingest(preview_metadata_artifact)

        # THEN
        s3_crawler.search_files.assert_called_once_with(
            snapshot_names,
            rcc_s3_params,
            match_to_file=preview_metadata_ingestor.metadata_chunk_match)
        preview_metadata_ingestor._get_metacontent_chunks.assert_called_once_with(
            metadata_chunks_keys)
        metadata_merger.merge_metadata_chunks.assert_called_once_with(downloaded_metachunks)
        preview_metadata_ingestor._upload_metacontent_to_devcloud.assert_called_once_with(
            devcloud_content)
        assert devcloud_path_uploaded == preview_metadata_artifact.s3_path
