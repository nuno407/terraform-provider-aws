# mypy: disable-error-code=attr-defined
"""Unit tests for the SnapshotIngestor class."""
import re
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock

from pytest import fixture, mark, raises
from pytest_lazyfixture import lazy_fixture
from pytz import UTC

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (Artifact, MetadataType, RecorderType,
                                  S3VideoArtifact, SignalsArtifact,
                                  SnapshotArtifact, TimeWindow)
from sdretriever.config import SDRetrieverConfig
from sdretriever.exceptions import FileAlreadyExists
from sdretriever.ingestor.snapshot_metadata import SnapshotMetadataIngestor

SNAP_TIME = datetime.now(tz=UTC) - timedelta(hours=4)
UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
CREDENTIALS = Mock()
TENANT_ID = "foo"
DEVICE_ID = "bar"
UID = "baz"
WIDTH = 1920
HEIGHT = 1080
EXPECTED_FILE_NAME = f"{TENANT_ID}/{TENANT_ID}_{DEVICE_ID}_{UID}_{round(SNAP_TIME.timestamp()*1000)}_metadata_full.json"


class TestMetadataSnapshotIngestor():
    """Unit tests for the SnapshotIngestor class."""

    @fixture()
    def snap_metadata_ingestor(
            self,
            config: SDRetrieverConfig,
            container_services: ContainerServices,
            rcc_client_factory: S3ClientFactory,
            s3_controller: S3Controller) -> SnapshotMetadataIngestor:
        """SnapshotIngestor under test."""
        return SnapshotMetadataIngestor(
            container_services,
            rcc_client_factory,
            s3_controller,
            config,
            Mock()
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
            uuid=UID,
            end_timestamp=SNAP_TIME
        )

    @fixture()
    def snapshot_metadata_artifact(self, snapshot_artifact: SnapshotArtifact) -> SignalsArtifact:
        """SnapshotSignalsArtifact for testing."""
        return SignalsArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            metadata_type=MetadataType.SIGNALS,
            referred_artifact=snapshot_artifact
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
            footage_id="1741741b-162a-50ef-a1cd-fbc3caabb3f1",
            rcc_s3_path="s3://rcc-bucket/key",
            end_timestamp=SNAP_TIME
        )

    @mark.unit()
    @mark.parametrize("artifact", [
        (lazy_fixture("video_artifact")),
        (lazy_fixture("snapshot_artifact"))
    ])
    def test_non_snap_raises_exception(self, snap_metadata_ingestor: SnapshotMetadataIngestor,
                                       artifact: Artifact) -> None:
        """Non-video artifacts should raise an exception."""
        with raises(ValueError):
            snap_metadata_ingestor.ingest(artifact)

    @mark.unit()
    def test_snap_metadata_already_exists(self, snap_metadata_ingestor: SnapshotMetadataIngestor,
                                          snapshot_metadata_artifact: SignalsArtifact,
                                          s3_controller: S3Controller,
                                          raw_s3: str) -> None:
        """Snapshots that already exist should raise an exception."""
        # GIVEN
        s3_controller.check_s3_file_exists = Mock(return_value=True)  # type: ignore[method-assign]

        # WHEN
        with raises(FileAlreadyExists):
            snap_metadata_ingestor.ingest(snapshot_metadata_artifact)

        # THEN
        s3_controller.check_s3_file_exists.assert_called_once_with(
            raw_s3,
            EXPECTED_FILE_NAME
        )

    @mark.unit()
    def test_successful_ingestion(self,
                                  snap_metadata_ingestor: SnapshotMetadataIngestor,
                                  snapshot_metadata_artifact: SignalsArtifact,
                                  s3_controller: S3Controller,
                                  raw_s3: str,
                                  rcc_bucket: str) -> None:
        """Successful ingestion should upload the snapshot."""
        # GIVEN
        s3_controller.check_s3_file_exists = Mock(return_value=False)  # type: ignore[method-assign]
        snap_metadata_ingestor.get_file_in_rcc = Mock(
            return_value=b'decompressed_mock')  # type: ignore[method-assign]

        # WHEN
        snap_metadata_ingestor.ingest(snapshot_metadata_artifact)

        # THEN
        s3_controller.check_s3_file_exists.assert_called_once_with(
            raw_s3,
            EXPECTED_FILE_NAME
        )
        snap_metadata_ingestor.get_file_in_rcc.assert_called_once_with(
            rcc_bucket,
            TENANT_ID,
            DEVICE_ID,
            UID,
            SNAP_TIME,
            ANY,
            [".json", ".json.zip"]
        )
        s3_controller.upload_file.assert_called_once_with(
            b'decompressed_mock',
            raw_s3,
            EXPECTED_FILE_NAME
        )
