from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock

from pytest import fixture, mark, raises
from pytz import UTC

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (IMUArtifact, RecorderType, SnapshotArtifact,
                                  TimeWindow)
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.ingestor.imu import IMUIngestor
from sdretriever.ingestor.metacontent import MetacontentChunk
from sdretriever.s3_finder import S3Finder

RAW_S3 = "raw-s3"
CHUNK_PATHS = [
    "TrainingRecorder_TrainingRecorder1.mp4._a_b_1234_imu_raw.csv.zip",
    "TrainingRecorder_TrainingRecorder1.mp4._a_b_5678_imu_raw.csv.zip",
]
CHUNKS = [
    MetacontentChunk(filename=CHUNK_PATHS[0], data=b"1"),
    MetacontentChunk(filename=CHUNK_PATHS[1], data=b"2")
]
SNAP_TIME = datetime.now(tz=UTC) - timedelta(hours=4)
UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
TENANT_ID = "foo"
DEVICE_ID = "bar"
UID = "baz"
ART_ID = f"{TENANT_ID}_{DEVICE_ID}_{UID}_{round(SNAP_TIME.timestamp()*1000)}_IMU"


class TestImuIngestor:
    @fixture()
    def container_services(self) -> ContainerServices:
        container_services = Mock()
        type(container_services).raw_s3 = PropertyMock(return_value=RAW_S3)
        return container_services

    @fixture()
    def rcc_client_factory(self) -> S3ClientFactory:
        return Mock()

    @fixture()
    def s3_controller(self) -> S3Controller:
        return Mock()

    @fixture()
    def s3_finder(self) -> S3Finder:
        return Mock()

    @fixture()
    def imu_ingestor(
            self,
            container_services: ContainerServices,
            rcc_client_factory: S3ClientFactory,
            s3_controller: S3Controller,
            s3_finder: S3Finder) -> IMUIngestor:
        return IMUIngestor(
            container_services,
            rcc_client_factory,
            s3_controller,
            s3_finder
        )

    @fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            uuid=UID,
            recorder=RecorderType.SNAPSHOT,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            timestamp=SNAP_TIME
        )

    @fixture()
    def imu_artifact(self, snapshot_artifact: SnapshotArtifact) -> IMUArtifact:
        return IMUArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            referred_artifact=snapshot_artifact
        )

    @mark.unit()
    def test_other_artifacts_raise_error(self, snapshot_artifact, imu_ingestor):
        with raises(ValueError):
            imu_ingestor.ingest(snapshot_artifact)

    @mark.unit()
    def test_raise_if_not_all_parts_exist(self, imu_artifact: IMUArtifact, imu_ingestor: IMUIngestor):
        # GIVEN
        imu_ingestor._check_allparts_exist = Mock(return_value=(False, []))  # type: ignore[method-assign]

        # WHEN
        with raises(UploadNotYetCompletedError):
            imu_ingestor.ingest(imu_artifact)

    @mark.unit()
    def test_successful_ingestion(self, imu_artifact: IMUArtifact, imu_ingestor: IMUIngestor):
        # GIVEN
        imu_ingestor._check_allparts_exist = Mock(return_value=(True, CHUNK_PATHS))  # type: ignore[method-assign]
        imu_ingestor._get_metacontent_chunks = Mock(return_value=CHUNKS)  # type: ignore[method-assign]
        imu_ingestor._upload_metacontent_to_devcloud = Mock(  # type: ignore[method-assign]
            return_value=f"s3://{RAW_S3}/{imu_artifact.tenant_id}/{imu_artifact.artifact_id}.csv")

        # WHEN
        imu_ingestor.ingest(imu_artifact)

        # THEN
        imu_ingestor._check_allparts_exist.assert_called_once_with(imu_artifact)
        imu_ingestor._get_metacontent_chunks.assert_called_once_with(CHUNK_PATHS)
        upload_params = imu_ingestor._upload_metacontent_to_devcloud.call_args.args
        assert upload_params[0].data == b"12"
        assert imu_artifact.s3_path == f"s3://{RAW_S3}/{TENANT_ID}/{ART_ID}.csv"
