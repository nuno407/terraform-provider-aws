""" test handler. """
from unittest.mock import MagicMock, Mock, PropertyMock, call
from typing import Any
import pytest

from base.model.artifacts import (
    IMUArtifact,
    RecorderType,
    SnapshotArtifact,
    SignalsArtifact,
    IMUArtifact,
    VideoArtifact,
    EventArtifact,
    CameraServiceEventArtifact,
    DeviceInfoEventArtifact,
    CameraBlockedOperatorArtifact,
    SOSOperatorArtifact,
    PeopleCountOperatorArtifact,
    IncidentEventArtifact)
from sanitizer.artifact.artifact_controller import ArtifactController
from sanitizer.message.message_controller import MessageController
from base.aws.model import SQSMessage
from base.aws.sqs import SQSController
from sanitizer.handler import Handler, ArtifactDispatch
from sanitizer.device_info_db_client import DeviceInfoDBClient


@pytest.fixture
def mock_metadata_controller() -> SQSController:
    """ metadata controller mock. """
    return Mock()


@pytest.fixture
def mock_sqs_controller() -> SQSController:
    """ sqs controller mock. """
    return Mock()


@pytest.fixture
def mock_message_controller() -> MessageController:
    """ message controller mock. """
    return Mock()


@pytest.fixture
def mock_artifact_controller() -> ArtifactController:
    """ artifact controller mock. """
    return Mock()


@pytest.fixture
def handler(
        mock_metadata_controller: SQSController,
        mock_sqs_controller: SQSController,
        mock_message_controller: MessageController,
        mock_artifact_controller: ArtifactController,
        mock_device_db_client: DeviceInfoDBClient) -> Handler:
    """ handler fixture. """
    return Handler(
        mock_metadata_controller,
        mock_sqs_controller,
        mock_message_controller,
        mock_artifact_controller,
        mock_device_db_client)


@pytest.mark.unit
def test_handler_run(mock_device_db_client: DeviceInfoDBClient):
    """ test handler run. """
    graceful_exit = MagicMock()
    prop = PropertyMock(side_effect=[True, False])
    type(graceful_exit).continue_running = prop
    sqs_controller = Mock()
    sqs_controller.get_message.return_value = "raw_sqs_message"
    message_parser = Mock()

    sqs_message = Mock()

    message_parser.parse.return_value = sqs_message
    message_filter = Mock()
    message_filter.is_relevant.return_value = sqs_message
    message_persistence = Mock()
    message_persistence.save.return_value = None

    message_controller = Mock()
    message_controller.parser = message_parser
    message_controller.filter = message_filter
    message_controller.persistence = message_persistence

    video_artifact = Mock(spec=VideoArtifact)
    video_artifact.device_id = None
    video_artifact.tenant_id = None
    video_artifact.recorder = RecorderType.INTERIOR
    snapshot_artifact = Mock(spec=SnapshotArtifact)
    snapshot_artifact.device_id = None
    snapshot_artifact.tenant_id = None
    snapshot_artifact.recorder = RecorderType.SNAPSHOT
    injected_artifact = Mock(spec=IMUArtifact)

    artifacts = [video_artifact, snapshot_artifact]
    artifact_parser = Mock()
    artifact_parser.parse.return_value = artifacts
    artifact_filter = Mock()
    artifact_filter.is_relevant.return_value = artifacts
    artifact_forwarder = Mock()
    artifact_forwarder.publish.return_value = None
    artifact_injector = Mock()
    artifact_injector.inject = Mock(side_effect=[[injected_artifact], []])

    artifact_controller = Mock()
    artifact_controller.parser = artifact_parser
    artifact_controller.filter = artifact_filter
    artifact_controller.forwarder = artifact_forwarder
    artifact_controller.injector = artifact_injector

    sqs_controller.delete_message.return_value = None

    metadata_sqs_controller = Mock()

    handler = Handler(metadata_sqs_controller,
                      sqs_controller,
                      message_controller,
                      artifact_controller,
                      mock_device_db_client)

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    message_persistence.save.assert_called_once_with(sqs_message)
    artifact_parser.parse.assert_called_once_with(sqs_message)
    artifact_filter.is_relevant.assert_has_calls([
        call(injected_artifact),
        call(video_artifact),
        call(snapshot_artifact),
    ])
    artifact_forwarder.publish.assert_has_calls([
        call(injected_artifact),
        call(video_artifact),
        call(snapshot_artifact)
    ])
    sqs_controller.delete_message.assert_called_once_with(sqs_message)


@pytest.mark.unit
def test_handler_run_no_raw_message(mock_device_db_client: DeviceInfoDBClient):
    """ test handler run. """
    graceful_exit = Mock()
    prop = PropertyMock(side_effect=[True, False])
    type(graceful_exit).continue_running = prop
    sqs_controller = Mock()
    sqs_controller.get_message.return_value = None
    metadata_sqs_controller = Mock()
    message_parser = Mock()

    message_parser.parse.return_value = None
    message_controller = Mock()
    message_controller.parser = message_parser

    handler = Handler(metadata_sqs_controller,
                      sqs_controller,
                      message_controller,
                      Mock(),
                      mock_device_db_client)

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_not_called()


@pytest.mark.unit
def test_handler_run_not_relevant(mock_device_db_client: DeviceInfoDBClient):
    """ test handler run. """
    graceful_exit = Mock()
    prop = PropertyMock(side_effect=[True, False])
    type(graceful_exit).continue_running = prop
    sqs_controller = Mock()
    sqs_controller.get_message.return_value = "raw_sqs_message"
    message_parser = Mock()
    metadata_sqs_controller = Mock()

    sqs_message = Mock()

    message_parser.parse.return_value = sqs_message
    message_filter = Mock()
    message_filter.is_relevant.return_value = None
    sqs_controller.delete_message.return_value = None

    message_controller = Mock()
    message_controller.parser = message_parser
    message_controller.filter = message_filter

    handler = Handler(metadata_sqs_controller,
                      sqs_controller,
                      message_controller,
                      Mock(),
                      mock_device_db_client)

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    sqs_controller.delete_message.assert_called_once_with(sqs_message)


@pytest.mark.unit
@pytest.mark.parametrize("image_base_type, is_relevant",
                         [(VideoArtifact, True), (SnapshotArtifact, True), (VideoArtifact, False), (SnapshotArtifact, False)])
def test_handle_image_base_artifact_is_irrelvant(
        image_base_type: Any,
        is_relevant: bool,
        handler: Handler,
        mock_artifact_controller: ArtifactController) -> None:
    """ test get injected artifacts. """
    video_artifact = Mock(spec=image_base_type)
    video_artifact.device_id = None
    video_artifact.tenant_id = None
    injected_artifact = Mock(spec=IMUArtifact)

    handler._Handler__get_injected_artifacts = Mock(return_value=ArtifactDispatch([injected_artifact], []))
    mock_artifact_controller.filter.is_relevant = Mock(return_value=is_relevant)

    artifacts = handler._Handler__handle_artifact(video_artifact)

    if not is_relevant:
        assert artifacts.sns_artifacts == [injected_artifact] and artifacts.sqs_artifacts == []
    else:
        assert artifacts.sns_artifacts == [injected_artifact, video_artifact] and artifacts.sqs_artifacts == []


@pytest.mark.unit
@pytest.mark.parametrize("event_type, is_relevant",
                         [(CameraServiceEventArtifact,
                           True),
                          (DeviceInfoEventArtifact,
                           True),
                             (IncidentEventArtifact,
                              True),
                             (CameraServiceEventArtifact,
                              False),
                             (DeviceInfoEventArtifact,
                              False),
                             (IncidentEventArtifact,
                              False)])
def test_handle_event_artifact(
        event_type: Any,
        is_relevant: bool,
        handler: Handler,
        mock_artifact_controller: ArtifactController,
        mock_device_db_client: DeviceInfoDBClient) -> None:
    """ test get injected artifacts. """
    artifact = Mock(spec=event_type)
    artifact.device_id = None
    artifact.tenant_id = None
    mock_device_db_client.store_device_information = Mock()
    mock_artifact_controller.filter.is_relevant = Mock(return_value=is_relevant)

    artifacts = handler._Handler__handle_artifact(artifact)

    if not is_relevant:
        mock_device_db_client.store_device_information.assert_not_called()
        assert artifacts.sns_artifacts == [] and artifacts.sqs_artifacts == []
        return

    if (event_type == DeviceInfoEventArtifact):
        mock_device_db_client.store_device_information.assert_called_once_with(artifact)
    else:
        mock_device_db_client.store_device_information.assert_not_called()

    assert artifacts.sns_artifacts == [] and artifacts.sqs_artifacts == [artifact]


@pytest.mark.unit
@pytest.mark.parametrize("event_type, is_relevant",
                         [(PeopleCountOperatorArtifact,
                           True),
                          (SOSOperatorArtifact,
                           True),
                             (CameraBlockedOperatorArtifact,
                              True),
                             (CameraBlockedOperatorArtifact,
                              False),
                             (SOSOperatorArtifact,
                              False),
                             (PeopleCountOperatorArtifact,
                              False)])
def test_handle_operator_artifact(
        event_type: Any,
        is_relevant: bool,
        handler: Handler,
        mock_artifact_controller: ArtifactController,
        mock_device_db_client: DeviceInfoDBClient) -> None:
    """ test get injected artifacts. """
    artifact = Mock(spec=event_type)
    artifact.device_id = None
    artifact.tenant_id = None
    mock_artifact_controller.filter.is_relevant = Mock(return_value=is_relevant)

    artifacts = handler._Handler__handle_artifact(artifact)

    if not is_relevant:
        assert artifacts.sns_artifacts == [] and artifacts.sqs_artifacts == []
        return

    assert artifacts.sns_artifacts == [artifact] and artifacts.sqs_artifacts == [artifact]


@pytest.mark.unit
@pytest.mark.parametrize("event_type, is_relevant",
                         [(SignalsArtifact, True), (IMUArtifact, True), (SignalsArtifact, False), (IMUArtifact, False)])
def test_handle_non_special_artifacts(
        event_type: Any,
        is_relevant: bool,
        handler: Handler,
        mock_artifact_controller: ArtifactController,
        mock_device_db_client: DeviceInfoDBClient) -> None:
    """ test get injected artifacts. """
    artifact = Mock(spec=event_type)
    artifact.device_id = None
    artifact.tenant_id = None
    mock_artifact_controller.filter.is_relevant = Mock(return_value=is_relevant)

    artifacts = handler._Handler__handle_artifact(artifact)

    if not is_relevant:
        assert artifacts.sns_artifacts == [] and artifacts.sqs_artifacts == []
        return

    assert artifacts.sns_artifacts == [artifact] and artifacts.sqs_artifacts == []


@pytest.mark.unit
def test_get_injected_artifacts(handler: Handler, mock_artifact_controller: ArtifactController) -> None:
    """ test get injected artifacts. """
    video_artifact = Mock(spec=VideoArtifact)
    video_artifact.device_id = None
    video_artifact.tenant_id = None
    injected_artifact = Mock(spec=IMUArtifact)
    injected_artifact2 = Mock(spec=SignalsArtifact)

    mock_artifact_controller.injector.inject = Mock(return_value=[injected_artifact, injected_artifact2])

    artifacts = handler._Handler__get_injected_artifacts(video_artifact)

    assert artifacts.sns_artifacts == [injected_artifact, injected_artifact2]
    assert artifacts.sqs_artifacts == []
