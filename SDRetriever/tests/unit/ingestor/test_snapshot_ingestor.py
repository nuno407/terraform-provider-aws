# mypy: disable-error-code=attr-defined
"""Unit tests for the SnapshotIngestor class."""
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, PropertyMock

from pytest import fixture, mark, raises
from pytz import UTC

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.aws.shared_functions import StsHelper
from base.model.artifacts import (RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from sdretriever.config import SDRetrieverConfig
from sdretriever.exceptions import FileAlreadyExists
from sdretriever.ingestor.post_processor import IVideoPostProcessor, VideoInfo
from sdretriever.ingestor.snapshot import SnapshotIngestor

QUEUE_NAME = "foo-queue"
SNAP_TIME = datetime.now(tz=UTC) - timedelta(hours=4)
UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
CREDENTIALS = Mock()
TENANT_ID = "foo"
DEVICE_ID = "bar"
UID = "baz"
WIDTH = 1920
HEIGHT = 1080
EXPECTED_FILE_NAME = f"{TENANT_ID}/{TENANT_ID}_{DEVICE_ID}_{UID}_{round(SNAP_TIME.timestamp()*1000)}.jpeg"
RAW_S3 = "raw-s3"


class TestSnapshotIngestor():
    """Unit tests for the SnapshotIngestor class."""
    @fixture()
    def config(self) -> SDRetrieverConfig:
        """Config for testing."""
        return SDRetrieverConfig(
            tenant_blacklist=[],
            recorder_blacklist=[],
            frame_buffer=0,
            training_whitelist=[],
            request_training_upload=True,
            discard_video_already_ingested=True,
            input_queue=QUEUE_NAME
        )

    @fixture()
    def container_services(self) -> ContainerServices:
        """ContainerServices for testing."""
        container_services = Mock()
        type(container_services).raw_s3 = PropertyMock(return_value=RAW_S3)
        type(container_services).rcc_info = PropertyMock(return_value={"s3_bucket": "rcc"})
        return container_services

    @fixture()
    def rcc_s3_client_factory(self) -> S3ClientFactory:
        """S3ClientFactory for testing."""
        return Mock()

    @fixture()
    def s3_controller(self) -> S3Controller:
        """S3Controller for testing."""
        s3_controller = Mock()
        s3_controller.upload_file = Mock()
        return s3_controller

    @fixture()
    def snap_ingestor(
            self,
            config: SDRetrieverConfig,
            container_services: ContainerServices,
            rcc_s3_client_factory: S3ClientFactory,
            s3_controller: S3Controller) -> SnapshotIngestor:
        """SnapshotIngestor under test."""
        return SnapshotIngestor(
            container_services,
            rcc_s3_client_factory,
            s3_controller,
            config,
            s3_finder=Mock()
        )

    @fixture()
    def video_artifact(self) -> S3VideoArtifact:
        """S3VideoArtifact for testing."""
        return S3VideoArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            recorder=RecorderType.INTERIOR,
            timestamp=SNAP_TIME,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            footage_id="babe76af-6dd8-533c-9ac3-2295f5fd779d",
            rcc_s3_path="s3://rcc-bucket/key",
            end_timestamp=SNAP_TIME
        )

    @fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            recorder=RecorderType.SNAPSHOT,
            timestamp=SNAP_TIME,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            uuid=UID
        )

    @mark.unit()
    def test_non_snap_raises_exception(self, snap_ingestor: SnapshotIngestor,
                                       video_artifact: S3VideoArtifact) -> None:
        """Non-video artifacts should raise an exception."""
        with raises(ValueError):
            snap_ingestor.ingest(video_artifact)

    @mark.unit()
    def test_snap_already_exists(self, snap_ingestor: SnapshotIngestor,
                                 snapshot_artifact: SnapshotArtifact,
                                 s3_controller: S3Controller) -> None:
        """Snapshots that already exist should raise an exception."""
        # GIVEN
        s3_controller.check_s3_file_exists = Mock(return_value=True)  # type: ignore[method-assign]

        # WHEN
        with raises(FileAlreadyExists):
            snap_ingestor.ingest(snapshot_artifact)

        # THEN
        s3_controller.check_s3_file_exists.assert_called_once_with(
            RAW_S3,
            EXPECTED_FILE_NAME
        )

    @mark.unit()
    def test_successful_ingestion(self, snap_ingestor: SnapshotIngestor,
                                  snapshot_artifact: SnapshotArtifact,
                                  s3_controller: S3Controller) -> None:
        """Successful ingestion should upload the snapshot."""
        # GIVEN
        s3_controller.check_s3_file_exists = Mock(return_value=False)  # type: ignore[method-assign]
        snap_ingestor.get_file_in_rcc = Mock(return_value=b'')  # type: ignore[method-assign]

        # WHEN
        snap_ingestor.ingest(snapshot_artifact)

        # THEN
        s3_controller.check_s3_file_exists.assert_called_once_with(
            RAW_S3,
            EXPECTED_FILE_NAME
        )
        snap_ingestor.get_file_in_rcc.assert_called_once_with(
            'rcc',
            TENANT_ID,
            DEVICE_ID,
            UID,
            SNAP_TIME,
            ANY,
            [".jpeg", ".png"]
        )
        s3_controller.upload_file.assert_called_once_with(
            b'',
            RAW_S3,
            EXPECTED_FILE_NAME
        )
