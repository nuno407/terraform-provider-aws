# type: ignore
import re
from pytz import UTC
from datetime import datetime
from copy import deepcopy

from unittest.mock import Mock, PropertyMock
from pytest import fixture
from mypy_boto3_s3 import S3Client
from sdretriever.config import SDRetrieverConfig
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from sdretriever.s3.s3_finder_rcc import S3FinderRCC
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.ingestor.post_processor import FFProbeExtractorPostProcessor
from base.model.artifacts import TimeWindow, RecorderType, SnapshotArtifact, S3VideoArtifact, Recording, PreviewSignalsArtifact, MultiSnapshotArtifact, SignalsArtifact


@fixture()
def config() -> SDRetrieverConfig:
    """Config for testing."""
    return SDRetrieverConfig(
        tenant_blacklist=[],
        recorder_blacklist=[],
        training_whitelist=[],
        discard_already_ingested=True,
        input_queue="queue",
        temporary_bucket="tmp_bucket"
    )


@fixture()
def metadata_merger() -> MetadataMerger:
    return Mock()


@fixture()
def raw_s3() -> str:
    return "raw-s3"


@fixture()
def rcc_bucket() -> str:
    return "test-rcc-bucket"


@fixture()
def mock_start_timestamp() -> datetime:
    return datetime(year=2023, month=2, day=2, hour=10, minute=10, tzinfo=UTC)


@fixture()
def mock_stop_timestamp() -> datetime:
    return datetime(year=2023, month=2, day=2, hour=10, minute=15, tzinfo=UTC)


@fixture()
def mock_upload_timewindow() -> TimeWindow:
    return TimeWindow(
        start=datetime(year=2023, month=2, day=2, hour=11, minute=15, tzinfo=UTC),
        end=datetime(year=2023, month=2, day=2, hour=11, minute=16, tzinfo=UTC)
    )


@fixture()
def mock_device_id() -> str:
    return "DATANAUTS_DEV_01"


@fixture()
def mock_tenant_id() -> str:
    return "datanatus"


@fixture()
def snapshot_uuid() -> str:
    return "baz"


@fixture()
def snapshot_artifact(
        mock_device_id: str,
        mock_tenant_id: str,
        mock_start_timestamp: datetime,
        mock_upload_timewindow: TimeWindow,
        snapshot_uuid: str) -> SnapshotArtifact:
    return SnapshotArtifact(
        tenant_id=mock_tenant_id,
        device_id=mock_device_id,
        uuid=snapshot_uuid,
        recorder=RecorderType.SNAPSHOT,
        upload_timing=mock_upload_timewindow,
        timestamp=mock_start_timestamp,
        end_timestamp=mock_start_timestamp
    )


@fixture()
def snapshot_metadata_artifact(
        mock_device_id: str,
        mock_tenant_id: str,
        snapshot_artifact: SnapshotArtifact) -> SignalsArtifact:
    """S3VideoArtifact for testing."""

    return SignalsArtifact(
        tenant_id=mock_tenant_id,
        device_id=mock_device_id,
        referred_artifact=snapshot_artifact)


@fixture()
def preview_metadata_artifact(
        mock_device_id: str,
        mock_tenant_id: str,
        mock_start_timestamp: datetime,
        mock_upload_timewindow: TimeWindow,
        mock_stop_timestamp: datetime,
        rcc_bucket: str,
        snapshot_artifact: SnapshotArtifact) -> PreviewSignalsArtifact:
    """S3VideoArtifact for testing."""

    recording_id = "InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76"

    snapshot_1 = deepcopy(snapshot_artifact)
    snapshot_1.uuid = f"InteriorRecorderPreview_{recording_id}_1.jpeg"
    snapshot_2 = deepcopy(snapshot_artifact)
    snapshot_2.uuid = f"InteriorRecorderPreview_{recording_id}_2.jpeg"

    return PreviewSignalsArtifact(
        tenant_id=mock_tenant_id,
        device_id=mock_device_id,
        recorder=RecorderType.INTERIOR,
        timestamp=mock_start_timestamp,
        upload_timing=mock_upload_timewindow,
        end_timestamp=mock_stop_timestamp,
        referred_artifact=MultiSnapshotArtifact(
            tenant_id=mock_tenant_id,
            device_id=mock_device_id,
            recorder=RecorderType.INTERIOR_PREVIEW,
            timestamp=mock_start_timestamp,
            end_timestamp=mock_stop_timestamp,
            upload_timing=mock_upload_timewindow,
            chunks=[
                snapshot_1,
                snapshot_2
            ],
            recording_id=recording_id
        ))


@fixture()
def interior_video_artifact(
        mock_device_id: str,
        mock_tenant_id: str,
        mock_start_timestamp: datetime,
        mock_upload_timewindow: TimeWindow,
        mock_stop_timestamp: datetime,
        rcc_bucket: str) -> S3VideoArtifact:
    """S3VideoArtifact for testing."""
    return S3VideoArtifact(
        tenant_id=mock_tenant_id,
        device_id=mock_device_id,
        recorder=RecorderType.INTERIOR,
        timestamp=mock_start_timestamp,
        upload_timing=mock_upload_timewindow,
        footage_id="baz_footage",
        rcc_s3_path=f"s3://{rcc_bucket}/video.mp4",
        end_timestamp=mock_stop_timestamp,
        recordings=[
            Recording(
                recording_id="InteriorRecorder-abc",
                chunk_ids=[
                    1,
                    2,
                    3]),
            Recording(
                recording_id="InteriorRecorder-abc1",
                chunk_ids=[1])])


@fixture()
def training_video_artifact(
        mock_device_id: str,
        mock_tenant_id: str,
        mock_start_timestamp: datetime,
        mock_upload_timewindow: TimeWindow,
        mock_stop_timestamp: datetime,
        rcc_bucket: str) -> S3VideoArtifact:
    """S3VideoArtifact for testing."""
    return S3VideoArtifact(
        tenant_id=mock_tenant_id,
        device_id=mock_device_id,
        recorder=RecorderType.TRAINING,
        timestamp=mock_start_timestamp,
        upload_timing=mock_upload_timewindow,
        footage_id="baz_footage",
        rcc_s3_path=f"s3://{rcc_bucket}/video.mp4",
        end_timestamp=mock_stop_timestamp,
        recordings=[
            Recording(
                recording_id="TrainingRecorder-abc",
                chunk_ids=[
                    1,
                    2,
                    3]),
            Recording(
                recording_id="TrainingRecorder-abc1",
                chunk_ids=[1])])


@fixture()
def ffprobe_post_processor() -> FFProbeExtractorPostProcessor:
    return Mock()


@fixture()
def container_services(raw_s3, rcc_bucket) -> ContainerServices:
    container_services = Mock()
    type(container_services).raw_s3 = PropertyMock(return_value=raw_s3)
    type(container_services).rcc_info = {
        "s3_bucket": rcc_bucket,
    }
    return container_services


@fixture()
def rcc_client_factory() -> S3ClientFactory:
    return Mock()


@fixture()
def s3_controller() -> S3Controller:
    """S3Controller for testing."""
    s3_controller = Mock()
    s3_controller.upload_file = Mock()
    return s3_controller


@fixture()
def s3_finder() -> S3FinderRCC:
    return Mock()


@fixture()
def s3_client() -> S3Client:
    return Mock()


@fixture()
def s3_crawler() -> S3CrawlerRCC:
    return Mock()


@fixture()
def rcc_chunk_downloader() -> RCCChunkDownloader:
    return Mock()


@fixture()
def s3_downloader_uploader() -> S3DownloaderUploader:
    return Mock()


@fixture()
def metadata_merger() -> MetadataMerger:
    return MetadataMerger()


@fixture()
def video_pattern() -> re.Pattern:
    return re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")


@fixture()
def image_pattern() -> re.Pattern:
    return re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.jpeg$")


@fixture
def s3_client_factory():
    s3_client = Mock()

    def s3_controller_factory():
        return s3_client
    return s3_controller_factory


@fixture
def s3_controller_factory():
    s3_client = Mock()

    def s3_controller_factory():
        return s3_client
    return s3_controller_factory


@fixture
def s3_controller(s3_controller_factory: Mock) -> Mock:
    return s3_controller_factory()
