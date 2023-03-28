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

    video_artifact = Mock()
    snapshot_artifact = Mock()
    artifacts = [video_artifact, snapshot_artifact]

    artifact_parser = Mock()
    artifact_parser.parse.return_value = artifacts
    artifact_filter = Mock()
    artifact_filter.apply.return_value = artifacts
    forwarder = Mock()
    publish_mock = Mock(return_value=None)
    forwarder.publish = publish_mock
    sqs_controller.delete_message.return_value = None

    handler = Handler(sqs_controller,
                      message_parser,
                      message_filter,
                      artifact_filter,
                      artifact_parser,
                      forwarder)
    test_helper_continue_running = Mock(side_effect=[True, False])
    handler.run(graceful_exit, test_helper_continue_running)

    sqs_controller.get_queue_url.assert_called_once()
    sqs_controller.get_message.assert_called_once_with("queue_url")
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    artifact_parser.parse.assert_called_once_with(sqs_message)
    artifact_filter.apply.assert_called_once_with(artifacts)
    publish_mock.assert_has_calls([call(video_artifact), call(snapshot_artifact)])
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

    handler = Handler(sqs_controller,
                      message_parser,
                      Mock(),
                      Mock(),
                      Mock(),
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

    handler = Handler(sqs_controller,
                      message_parser,
                      message_filter,
                      Mock(),
                      Mock(),
                      Mock())
    test_helper_continue_running = Mock(side_effect=[True, False])
    handler.run(graceful_exit, test_helper_continue_running)

    sqs_controller.get_queue_url.assert_called_once()
    sqs_controller.get_message.assert_called_once_with("queue_url")
    message_parser.parse.assert_called_once_with("raw_sqs_message")
    message_filter.is_relevant.assert_called_once_with(sqs_message)
    sqs_controller.delete_message.assert_called_once_with("queue_url", sqs_message)
