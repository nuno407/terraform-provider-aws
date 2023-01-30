""" Selector Tests. """
import json
from unittest import mock

import pytest
from selector.selector import Selector


@pytest.mark.unit
class TestSelector():
    """ Tests on Selector Component. """

    @mock.patch("base.aws.container_services.ContainerServices", autospec=True)
    def test_correct_call_of_footage_api_handle_hq_queue(self, mock_container_services):
        """ Tests handle_hq_queue message handler. """
        # GIVEN
        mock_footage_api_wrapper = mock.Mock()
        mock_footage_api_wrapper.request_footage = mock.Mock()
        selector = Selector(None, mock_container_services, mock_footage_api_wrapper, "super_special_queue")
        body = {
            "deviceId": "test",
            "footageFrom": 1234567,
            "footageTo": 1234569
        }
        message = {
            "Body": json.dumps(body).replace("\"", "'"),
            "ReceiptHandle": "receipt_handle"
        }

        mock_container_services.listen_to_input_queue.return_value = message
        mock_container_services.get_message_body.return_value = body

        # WHEN
        selector.handle_hq_queue()

        # THEN
        mock_container_services.listen_to_input_queue.assert_called_once()
        mock_container_services.delete_message.assert_called_once_with(None, "receipt_handle", "super_special_queue")
        mock_footage_api_wrapper.request_footage.assert_called_once_with("test", 1234567, 1234569)

    @mock.patch("base.aws.container_services.ContainerServices", autospec=True)
    def test_incorrect_call_of_process_hq_message(self, mock_container_services):
        """ Tests handle_hq_queue and __process_hq_message logic. Should not delete message when request fails """
        # GIVEN
        mock_footage_api_wrapper = mock.Mock()
        mock_footage_api_wrapper.request_footage = mock.Mock(
            side_effect=RuntimeError)  # simulate request fail with raising runtime error
        selector = Selector(None, mock_container_services, mock_footage_api_wrapper, "super_special_queue")
        body = {
            "deviceId": "test",
            "footageFrom": 1234567,
            "footageTo": 1234569
        }
        message = {
            "Body": json.dumps(body).replace("\"", "'"),
            "ReceiptHandle": "receipt_handle"
        }

        mock_container_services.listen_to_input_queue.return_value = message
        mock_container_services.get_message_body.return_value = body

        # WHEN
        selector.handle_hq_queue()
        # THEN
        mock_container_services.listen_to_input_queue.assert_called_once()
        try:
            mock_container_services.delete_message.assert_called_once_with(
                None, "receipt_handle", "super_special_queue")
        except AssertionError:
            pass
        mock_footage_api_wrapper.request_footage.assert_called_once_with("test", 1234567, 1234569)
