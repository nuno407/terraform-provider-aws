""" test handler. """
from unittest.mock import MagicMock, Mock, PropertyMock, call

import pytest

from sanitizer.handler import Handler


@pytest.mark.unit
def test_handler_run():
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

    video_artifact = Mock()
    snapshot_artifact = Mock()
    injected_artifact = Mock()

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

    handler = Handler(sqs_controller,
                      message_controller,
                      artifact_controller)

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    message_persistence.save.assert_called_once_with(sqs_message)
    artifact_parser.parse.assert_called_once_with(sqs_message)
    artifact_filter.is_relevant.assert_has_calls([
        call(video_artifact),
        call(injected_artifact),
        call(snapshot_artifact),
    ])
    artifact_forwarder.publish.assert_has_calls([
        call(video_artifact),
        call(injected_artifact),
        call(snapshot_artifact)
    ])
    sqs_controller.delete_message.assert_called_once_with(sqs_message)


@pytest.mark.unit
def test_handler_run_no_raw_message():
    """ test handler run. """
    graceful_exit = Mock()
    prop = PropertyMock(side_effect=[True, False])
    type(graceful_exit).continue_running = prop
    sqs_controller = Mock()
    sqs_controller.get_message.return_value = None
    message_parser = Mock()

    message_parser.parse.return_value = None
    message_controller = Mock()
    message_controller.parser = message_parser

    handler = Handler(sqs_controller,
                      message_controller,
                      Mock())

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_not_called()


@pytest.mark.unit
def test_handler_run_not_relevant():
    """ test handler run. """
    graceful_exit = Mock()
    prop = PropertyMock(side_effect=[True, False])
    type(graceful_exit).continue_running = prop
    sqs_controller = Mock()
    sqs_controller.get_message.return_value = "raw_sqs_message"
    message_parser = Mock()

    sqs_message = Mock()

    message_parser.parse.return_value = sqs_message
    message_filter = Mock()
    message_filter.is_relevant.return_value = None
    sqs_controller.delete_message.return_value = None

    message_controller = Mock()
    message_controller.parser = message_parser
    message_controller.filter = message_filter

    handler = Handler(sqs_controller,
                      message_controller,
                      Mock())

    handler.run(graceful_exit)

    sqs_controller.get_message.assert_called_once()
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    sqs_controller.delete_message.assert_called_once_with(sqs_message)
