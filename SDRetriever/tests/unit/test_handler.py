from datetime import datetime
from unittest.mock import MagicMock, Mock, call

import pytest
import pytz

from base.model.artifacts import (IMUArtifact, RecorderType, SignalsArtifact,
                                  SnapshotArtifact, TimeWindow, VideoArtifact)
from sdretriever.constants import CONTAINER_NAME
from sdretriever.handler import IngestionHandler


@pytest.fixture
def imu_ing():
    """IMUIngestor fixture
    """
    return Mock()


@pytest.fixture
def video_metadata_ing():
    """MetadataIngestor fixture
    """
    return Mock()


@pytest.fixture
def video_ing():
    """VideoIngestor fixture
    """
    return Mock()


@pytest.fixture
def snap_ing():
    """SnapshotIngestor fixture
    """
    return Mock()


@pytest.fixture
def cont_services():
    """ContainerServices fixture
    """
    cont_services = Mock()
    cont_services.sqs_queues_list = {
        "Metadata": "metadata_queue",
        "HQ_Selector": "hq_request_queue",
        "MDFParser": "mdfp_queue"
    }
    yield cont_services


@pytest.fixture
def config():
    """SDRetrieverConfig fixture
    """
    return Mock()


@pytest.fixture
def sqs_controller():
    """SQSController fixture
    """
    return Mock()


@pytest.fixture
def snap_metadata_ing():
    """SnapshotMetadataIngestor fixture"""
    return Mock()


@pytest.fixture
def ingestion_handler(imu_ing,
                      video_metadata_ing,
                      video_ing,
                      snap_ing,
                      snap_metadata_ing,
                      cont_services,
                      config,
                      sqs_controller):
    """IngestionHandler fixture
    """
    return IngestionHandler(imu_ing,
                            video_metadata_ing,
                            video_ing,
                            snap_ing,
                            snap_metadata_ing,
                            cont_services,
                            config,
                            sqs_controller)


video_artifact = VideoArtifact(
    stream_name="stream_name",
    recorder=RecorderType.INTERIOR,
    upload_timing=TimeWindow(start=datetime.now(
        tz=pytz.UTC), end=datetime.now(tz=pytz.UTC)),
    tenant_id="tenant_id",
    device_id="device_id",
    timestamp=datetime.now(tz=pytz.UTC),
    end_timestamp=datetime.now(tz=pytz.UTC)
)


@pytest.mark.unit
def test_ingestion_handler_handle_video(ingestion_handler: IngestionHandler,
                                        video_ing: Mock,
                                        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    serialized_video_artifact = video_artifact.stringify()

    ingestion_handler.handle(video_artifact, message)

    video_ing.ingest.assert_called_once_with(video_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(serialized_video_artifact, CONTAINER_NAME, "metadata_queue"),
        call(serialized_video_artifact, CONTAINER_NAME, "hq_request_queue"),
    ])


snapshot_artifact = SnapshotArtifact(
    recorder=RecorderType.SNAPSHOT,
    upload_timing=TimeWindow(start=datetime.now(
        tz=pytz.UTC), end=datetime.now(tz=pytz.UTC)),
    tenant_id="tenant_id",
    device_id="device_id",
    timestamp=datetime.now(tz=pytz.UTC),
    uuid="uuid"
)


@pytest.mark.unit
def test_ingestion_handler_handle_snapshot(ingestion_handler: IngestionHandler,
                                           snap_ing: Mock,
                                           sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()

    ingestion_handler.handle(snapshot_artifact, message)

    snap_ing.ingest.assert_called_once_with(snapshot_artifact)
    sqs_controller.send_message.assert_called_once_with(
        snapshot_artifact.stringify(), CONTAINER_NAME, "metadata_queue")


imu_artifact = IMUArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=video_artifact
)


@pytest.mark.unit
def test_ingestion_handler_handle_imu(ingestion_handler: IngestionHandler,
                                      imu_ing: Mock,
                                      sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()

    ingestion_handler.handle(imu_artifact, message)

    imu_ing.ingest.assert_called_once_with(imu_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(imu_artifact.stringify(), CONTAINER_NAME, "mdfp_queue"),
    ])


signals_artifact = SignalsArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=video_artifact
)


@pytest.mark.unit
def test_ingestion_handler_handle_signals(
        ingestion_handler: IngestionHandler,
        video_metadata_ing: Mock,
        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()

    ingestion_handler.handle(signals_artifact, message)

    video_metadata_ing.ingest.assert_called_once_with(signals_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(signals_artifact.stringify(), CONTAINER_NAME, "mdfp_queue"),
    ])


snapshot_signals_artifact = SignalsArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=snapshot_artifact
)


@pytest.mark.unit
def test_ingestion_handler_handle_signals_snapshot(
        ingestion_handler: IngestionHandler,
        snap_metadata_ing: Mock,
        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()

    ingestion_handler.handle(snapshot_signals_artifact, message)

    snap_metadata_ing.ingest.assert_called_once_with(snapshot_signals_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(snapshot_signals_artifact.stringify(),
             CONTAINER_NAME, "metadata_queue")
    ])