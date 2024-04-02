from datetime import datetime
from unittest.mock import MagicMock, Mock, call

import pytest
import pytz

from base.model.artifacts import (IMUArtifact,
                                  RecorderType, S3VideoArtifact,
                                  SignalsArtifact, SnapshotArtifact,
                                  TimeWindow, Recording)
from base.model.artifacts import (IMUArtifact, RecorderType, SignalsArtifact, MultiSnapshotArtifact,
                                  SnapshotArtifact, TimeWindow, PreviewSignalsArtifact)
from sdretriever.constants import CONTAINER_NAME
from sdretriever.handler import IngestionHandler
from sdretriever.exceptions import EmptyFileError

datetime_in_past = datetime(2023, 5, 10, 2, 10, tzinfo=pytz.UTC)


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
def preview_metadata_ing():
    """MetadataIngestor fixture
    """
    return Mock()


@pytest.fixture
def s3_video_ing():
    """S3VideoIngestor fixture
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
        "Selector": "selector_queue",
        "MDFParser": "mdfp_queue"
    }
    yield cont_services


@pytest.fixture
def config_discard_ingested():
    """SDRetrieverConfig fixture
    """
    mock = Mock()
    mock.discard_already_ingested = True
    return mock


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
                      s3_video_ing,
                      snap_ing,
                      snap_metadata_ing,
                      preview_metadata_ing,
                      cont_services,
                      config_discard_ingested,
                      sqs_controller):
    """IngestionHandler fixture
    """
    return IngestionHandler(imu_ing,
                            video_metadata_ing,
                            s3_video_ing,
                            snap_ing,
                            snap_metadata_ing,
                            preview_metadata_ing,
                            cont_services,
                            config_discard_ingested,
                            sqs_controller)


s3_video_artifact = S3VideoArtifact(
    artifact_id="bar",
    raw_s3_path="s3://raw/foo/bar.something",
    anonymized_s3_path="s3://anonymized/foo/bar.something",
    recorder=RecorderType.INTERIOR,
    upload_timing=TimeWindow(start=datetime_in_past, end=datetime_in_past),
    tenant_id="tenant_id",
    device_id="device_id",
    footage_id="footage_id",
    rcc_s3_path="s3://bucket/key",
    timestamp=datetime_in_past,
    end_timestamp=datetime_in_past,
    recordings=[
        Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
)


@pytest.mark.unit
def test_ingestion_handler_handle_s3_video(ingestion_handler: IngestionHandler,
                                           s3_video_ing: Mock,
                                           sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    s3_video_ing.is_already_ingested = Mock(return_value=False)
    serialized_video_artifact = s3_video_artifact.stringify()

    ingestion_handler.handle(s3_video_artifact, message)

    s3_video_ing.ingest.assert_called_once_with(s3_video_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(serialized_video_artifact, CONTAINER_NAME, "metadata_queue")
    ])
    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_already_ingested_s3_video(ingestion_handler: IngestionHandler,
                                             s3_video_ing: Mock,
                                             sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    s3_video_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(s3_video_artifact, message)

    s3_video_ing.ingest.assert_not_called()
    s3_video_ing.is_already_ingested.assert_called_once_with(s3_video_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


snapshot_artifact = SnapshotArtifact(
    artifact_id="bar",
    raw_s3_path="s3://raw/foo/bar.something",
    anonymized_s3_path="s3://anonymized/foo/bar.something",
    recorder=RecorderType.SNAPSHOT,
    upload_timing=TimeWindow(start=datetime_in_past, end=datetime_in_past),
    end_timestamp=datetime_in_past,
    tenant_id="tenant_id",
    device_id="device_id",
    timestamp=datetime_in_past,
    uuid="uuid"
)


@pytest.mark.unit
def test_ingestion_handler_handle_snapshot(ingestion_handler: IngestionHandler,
                                           snap_ing: Mock,
                                           sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    snap_ing.is_already_ingested = Mock(return_value=False)

    ingestion_handler.handle(snapshot_artifact, message)

    snap_ing.ingest.assert_called_once_with(snapshot_artifact)
    sqs_controller.send_message.assert_called_once_with(
        snapshot_artifact.stringify(), CONTAINER_NAME, "metadata_queue")

    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_already_ingested_snapshot(ingestion_handler: IngestionHandler,
                                             snap_ing: Mock,
                                             sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    snap_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(snapshot_artifact, message)

    snap_ing.ingest.assert_not_called()
    snap_ing.is_already_ingested.assert_called_once_with(snapshot_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


imu_artifact = IMUArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=s3_video_artifact
)


@pytest.mark.skip("IMU Ingestion to mongodb is stopped")
def test_ingestion_handler_handle_imu(ingestion_handler: IngestionHandler,
                                      imu_ing: Mock,
                                      sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    imu_ing.is_already_ingested = Mock(return_value=False)

    ingestion_handler.handle(imu_artifact, message)

    imu_ing.ingest.assert_called_once_with(imu_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(imu_artifact.stringify(), CONTAINER_NAME, "mdfp_queue"),
    ])

    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_already_ingested_imu(ingestion_handler: IngestionHandler,
                                        imu_ing: Mock,
                                        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    imu_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(imu_artifact, message)

    imu_ing.ingest.assert_not_called()
    imu_ing.is_already_ingested.assert_called_once_with(imu_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


signals_artifact = SignalsArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=s3_video_artifact
)


@pytest.mark.unit
def test_ingestion_handler_handle_signals(
        ingestion_handler: IngestionHandler,
        video_metadata_ing: Mock,
        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    video_metadata_ing.is_already_ingested = Mock(return_value=False)

    ingestion_handler.handle(signals_artifact, message)

    video_metadata_ing.ingest.assert_called_once_with(signals_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(signals_artifact.stringify(), CONTAINER_NAME, "mdfp_queue"),
    ])

    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_already_ingested_signals(ingestion_handler: IngestionHandler,
                                            video_metadata_ing: Mock,
                                            sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    video_metadata_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(signals_artifact, message)

    video_metadata_ing.ingest.assert_not_called()
    video_metadata_ing.is_already_ingested.assert_called_once_with(
        signals_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


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
    snap_metadata_ing.is_already_ingested = Mock(return_value=False)

    ingestion_handler.handle(snapshot_signals_artifact, message)

    snap_metadata_ing.ingest.assert_called_once_with(snapshot_signals_artifact)
    sqs_controller.send_message.assert_has_calls([
        call(snapshot_signals_artifact.stringify(),
             CONTAINER_NAME, "metadata_queue")
    ])

    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_already_ingested_signals_snapshot(ingestion_handler: IngestionHandler,
                                                     snap_metadata_ing: Mock,
                                                     sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    snap_metadata_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(snapshot_signals_artifact, message)

    snap_metadata_ing.ingest.assert_not_called()
    snap_metadata_ing.is_already_ingested.assert_called_once_with(
        snapshot_signals_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


@pytest.mark.unit
def test_ingestion_empty_metadata(
        ingestion_handler: IngestionHandler,
        snap_metadata_ing: Mock,
        sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    snap_metadata_ing.is_already_ingested = Mock(return_value=False)
    snap_metadata_ing.ingest.side_effect = EmptyFileError("Empty file")

    ingestion_handler.handle(snapshot_signals_artifact, message)

    snap_metadata_ing.ingest.assert_called_once_with(snapshot_signals_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)


multisnapshot_artifact = MultiSnapshotArtifact(
    artifact_id="bar",
    tenant_id="tenant_id",
    device_id="device_id",
    timestamp=datetime_in_past,
    end_timestamp=datetime_in_past,
    recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
    upload_timing=TimeWindow(start=datetime_in_past, end=datetime_in_past),
    recorder=RecorderType.INTERIOR_PREVIEW,
    chunks=[snapshot_artifact])


preview_signals_artifact = PreviewSignalsArtifact(
    tenant_id="tenant_id",
    device_id="device_id",
    referred_artifact=multisnapshot_artifact,
    timestamp=datetime_in_past,
    end_timestamp=datetime_in_past
)


@pytest.mark.unit
def test_ingestion_handler_handle_signals_preview(
        ingestion_handler: IngestionHandler,
        preview_metadata_ing: Mock,
        sqs_controller: Mock,
        config_discard_ingested: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    config_discard_ingested.discard_already_ingested = False
    preview_metadata_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(preview_signals_artifact, message)
    preview_metadata_ing.ingest.assert_called_once_with(
        preview_signals_artifact)

    sqs_controller.send_message.assert_has_calls([
        call(preview_signals_artifact.stringify(),
             CONTAINER_NAME, "selector_queue")
    ])


@pytest.mark.unit
def test_ingestion_already_ingested_signals_preview(ingestion_handler: IngestionHandler,
                                                    preview_metadata_ing: Mock,
                                                    sqs_controller: Mock):
    """Test IngestionHandler.handle method
    """
    message = MagicMock()
    preview_metadata_ing.is_already_ingested = Mock(return_value=True)

    ingestion_handler.handle(preview_signals_artifact, message)

    preview_metadata_ing.ingest.assert_not_called()
    preview_metadata_ing.is_already_ingested.assert_called_once_with(
        preview_signals_artifact)
    sqs_controller.send_message.assert_not_called()
    sqs_controller.delete_message.assert_called_once_with(message)
