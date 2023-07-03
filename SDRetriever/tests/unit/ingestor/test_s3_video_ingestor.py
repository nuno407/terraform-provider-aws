# mypy: disable-error-code=attr-defined
"""Unit tests for the VideoIngestor class."""
from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock, patch

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
from sdretriever.ingestor.s3_video import S3VideoIngestor

QUEUE_NAME = "foo-queue"
VIDEO_START = datetime.now(tz=UTC) - timedelta(hours=4)
VIDEO_END = datetime.now(tz=UTC) - timedelta(hours=3)
UPLOAD_START = datetime.now(tz=UTC) - timedelta(hours=2)
UPLOAD_END = datetime.now(tz=UTC) - timedelta(hours=1)
CREDENTIALS = Mock()
TENANT_ID = "foo_tenant"
DEVICE_ID = "bar_device"
FOOTAGE_ID = "baz_footage"
RCC_BUCKET = "rcc_bucket"
RCC_KEY = f"{TENANT_ID}/{DEVICE_ID}/{FOOTAGE_ID}.mp4"

DURATION = 5.3
WIDTH = 1920
HEIGHT = 1080
EXPECTED_FILE_NAME = f"{TENANT_ID}/{DEVICE_ID}_{RecorderType.INTERIOR.value}_{FOOTAGE_ID}_{round(VIDEO_START.timestamp()*1000)}_{round(VIDEO_END.timestamp()*1000)}.mp4"
RAW_S3 = "raw-s3"


class TestVideoIngestor():
    """Unit tests for the VideoIngestor class."""
    # @fixture()
    # def config(self) -> SDRetrieverConfig:
    #     """Config for testing."""
    #     return SDRetrieverConfig(
    #         tenant_blacklist=[],
    #         recorder_blacklist=[],
    #         frame_buffer=0,
    #         training_whitelist=[],
    #         request_training_upload=True,
    #         discard_video_already_ingested=True,
    #         input_queue=QUEUE_NAME,
    #         ingest_from_kinesis=False,
    #         temporary_bucket=""
    #     )

    @fixture()
    def container_services(self) -> ContainerServices:
        """ContainerServices for testing."""
        container_services = Mock()
        type(container_services).raw_s3 = PropertyMock(return_value=RAW_S3)
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
    def post_processor(self) -> IVideoPostProcessor:
        """Post Processor for testing."""
        post_processor = Mock()
        post_processor.execute = Mock(
            return_value=VideoInfo(DURATION, WIDTH, HEIGHT))
        return post_processor

    @fixture()
    def video_ingestor(
            self,
            config: SDRetrieverConfig,
            container_services: ContainerServices,
            rcc_s3_client_factory: S3ClientFactory,
            s3_controller: S3Controller,
            post_processor: IVideoPostProcessor) -> S3VideoIngestor:
        """VideoIngestor under test."""
        return S3VideoIngestor(
            container_services,
            rcc_s3_client_factory,
            config,
            s3_controller,
            post_processor,
            s3_finder=Mock()
        )

    @fixture()
    def video_artifact(self) -> S3VideoArtifact:
        """S3VideoArtifact for testing."""
        return S3VideoArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            recorder=RecorderType.INTERIOR,
            timestamp=VIDEO_START,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            footage_id=FOOTAGE_ID,
            rcc_s3_path=f"s3://{RCC_BUCKET}/{RCC_KEY}",
            end_timestamp=VIDEO_END
        )

    @fixture()
    def snapshot_artifact(self) -> SnapshotArtifact:
        """SnapshotArtifact for testing."""
        return SnapshotArtifact(
            tenant_id=TENANT_ID,
            device_id=DEVICE_ID,
            recorder=RecorderType.SNAPSHOT,
            timestamp=VIDEO_START,
            upload_timing=TimeWindow(
                start=UPLOAD_START,
                end=UPLOAD_END
            ),
            uuid="baz",
            end_timestamp=VIDEO_END,
        )

    @mark.unit()
    def test_non_video_raises_exception(self, video_ingestor: S3VideoIngestor,
                                        snapshot_artifact: SnapshotArtifact) -> None:
        """Non-video artifacts should raise an exception."""
        with raises(ValueError):
            video_ingestor.ingest(snapshot_artifact)

    @mark.unit()
    def test_existing_video_is_discarded(self, s3_controller: S3Controller, video_ingestor: S3VideoIngestor,
                                         video_artifact: S3VideoArtifact) -> None:
        """Existing videos should be discarded."""
        # GIVEN
        s3_controller.check_s3_file_exists.return_value = True

        # WHEN-THEN
        with raises(FileAlreadyExists):
            video_ingestor.ingest(video_artifact)

        # THEN
        s3_controller.check_s3_file_exists.assert_called_once_with(
            RAW_S3, EXPECTED_FILE_NAME)

    @mark.unit()
    @patch("sdretriever.ingestor.s3_video.S3Controller")
    def test_successful_ingestion(
            self,
            s3_controller_mock: Mock,
            video_ingestor: S3VideoIngestor,
            s3_controller: S3Controller,
            post_processor: IVideoPostProcessor,
            rcc_s3_client_factory: S3ClientFactory,
            video_artifact: S3VideoArtifact) -> None:
        """Successful ingestion should call the post processor."""
        # GIVEN
        s3_controller.check_s3_file_exists.return_value = False
        rcc_s3_controller_mock = Mock()
        rcc_s3_controller_mock.download_file.return_value = b'foobar'
        s3_controller_mock.return_value = rcc_s3_controller_mock
        s3_controller_mock.get_s3_path_parts = S3Controller.get_s3_path_parts

        # WHEN
        video_ingestor.ingest(video_artifact)

        # THEN
        s3_controller_mock.assert_called_once_with(rcc_s3_client_factory.return_value)
        rcc_s3_controller_mock.download_file.assert_called_once_with(RCC_BUCKET, RCC_KEY)

        s3_controller.upload_file.assert_called_once_with(
            b'foobar', RAW_S3, EXPECTED_FILE_NAME)
        post_processor.execute.assert_called_once_with(b'foobar')

        assert video_artifact.actual_duration == DURATION
        assert video_artifact.resolution is not None
        assert video_artifact.resolution.width == WIDTH
        assert video_artifact.resolution.height == HEIGHT
        assert video_artifact.s3_path == f"s3://{RAW_S3}/{EXPECTED_FILE_NAME}"
