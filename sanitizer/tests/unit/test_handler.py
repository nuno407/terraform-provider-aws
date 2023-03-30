""" test handler. """
from unittest.mock import Mock, MagicMock, call

import pytest

from sanitizer.handler import Handler


@pytest.mark.unit
def test_handler_run():
    """ test handler run. """
    graceful_exit = MagicMock()
    graceful_exit.continue_running = True
    sqs_controller = Mock()
    sqs_controller.get_queue_url.return_value = "queue_url"
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
    artifacts = [video_artifact, snapshot_artifact]
    artifact_parser = Mock()
    artifact_parser.parse.return_value = artifacts
    artifact_filter = Mock()
    artifact_filter.is_relevant.return_value = artifacts
    artifact_forwarder = Mock()
    artifact_forwarder.publish.return_value = None

    artifact_controller = Mock()
    artifact_controller.parser = artifact_parser
    artifact_controller.filter = artifact_filter
    artifact_controller.forwarder = artifact_forwarder

    sqs_controller.delete_message.return_value = None

    handler = Handler(sqs_controller,
                      message_controller,
                      artifact_controller)

    test_helper_continue_running = Mock(side_effect=[True, False])

    handler.run(graceful_exit, test_helper_continue_running)

    sqs_controller.get_queue_url.assert_called_once()
    sqs_controller.get_message.assert_called_once_with("queue_url")
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    message_persistence.save.assert_called_once_with(sqs_message)
    artifact_parser.parse.assert_called_once_with(sqs_message)
    artifact_filter.is_relevant.assert_has_calls([call(video_artifact), call(snapshot_artifact)])
    artifact_forwarder.publish.assert_has_calls([call(video_artifact), call(snapshot_artifact)])
    sqs_controller.delete_message.assert_called_once_with("queue_url", sqs_message)


@pytest.mark.unit
def test_handler_run_no_raw_message():
    """ test handler run. """
    graceful_exit = MagicMock()
    graceful_exit.continue_running = True
    sqs_controller = Mock()
    sqs_controller.get_queue_url.return_value = "queue_url"
    sqs_controller.get_message.return_value = None
    message_parser = Mock()

    message_parser.parse.return_value = None
    message_controller = Mock()
    message_controller.parser = message_parser

    handler = Handler(sqs_controller,
                      message_controller,
                      Mock())

    test_helper_continue_running = Mock(side_effect=[True, False])

    handler.run(graceful_exit, test_helper_continue_running)

    sqs_controller.get_queue_url.assert_called_once()
    sqs_controller.get_message.assert_called_once_with("queue_url")
    message_parser.parse.assert_not_called()


@pytest.mark.unit
def test_handler_run_not_relevant():
    """ test handler run. """
    graceful_exit = MagicMock()
    graceful_exit.continue_running = True
    sqs_controller = Mock()
    sqs_controller.get_queue_url.return_value = "queue_url"
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

    test_helper_continue_running = Mock(side_effect=[True, False])

    handler.run(graceful_exit, test_helper_continue_running)

    sqs_controller.get_queue_url.assert_called_once()
    sqs_controller.get_message.assert_called_once_with("queue_url")
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    sqs_controller.delete_message.assert_called_once_with("queue_url", sqs_message)
