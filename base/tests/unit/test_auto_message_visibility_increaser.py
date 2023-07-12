from unittest.mock import Mock, patch
import pytest

from base.aws.auto_message_visibility_increaser import AutoMessageVisibilityIncreaser


@pytest.mark.unit
class TestAutoMessageVisibilityIncreaser():
    """Test AutoMessageVisibilityIncreaser."""

    @patch("base.aws.auto_message_visibility_increaser.time.sleep")
    @patch("base.aws.auto_message_visibility_increaser.multiprocessing.Process")
    def test_happy_path(self, multiprocessing_process_mock, _):
        sqs_client_mock = Mock()
        container_services_mock = Mock()
        multiprocessing_process_mock.return_value.start = Mock()
        multiprocessing_process_mock.return_value.kill = Mock()
        multiprocessing_process_mock.return_value.join = Mock()

        with AutoMessageVisibilityIncreaser(sqs_client_mock, "Receipt_handle", container_services_mock, 0, "input_queue"):
            pass

        multiprocessing_process_mock.assert_called_once()
        multiprocessing_process_mock.return_value.start.assert_called_once()
        multiprocessing_process_mock.return_value.kill.assert_called_once()
        multiprocessing_process_mock.return_value.join.assert_called_once()

    @patch("base.aws.auto_message_visibility_increaser.time.sleep")
    @patch("base.aws.auto_message_visibility_increaser.multiprocessing.Process")
    def test_unhappy_path(self, multiprocessing_process_mock, _):
        sqs_client_mock = Mock()
        container_services_mock = Mock()
        multiprocessing_process_mock.return_value.start = Mock()
        multiprocessing_process_mock.return_value.kill = Mock()
        multiprocessing_process_mock.return_value.join = Mock()

        try:
            with AutoMessageVisibilityIncreaser(sqs_client_mock, "Receipt_handle", container_services_mock, 0, "input_queue"):
                raise Exception("Test")
        except Exception:
            pass

        multiprocessing_process_mock.assert_called_once()
        multiprocessing_process_mock.return_value.start.assert_called_once()
        multiprocessing_process_mock.return_value.join.assert_called_once()
