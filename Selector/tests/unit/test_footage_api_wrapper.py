""" FootageApiWrapper Tests. """
import json
from unittest import mock
import pytest
from selector.footage_api_wrapper import FootageApiWrapper
from selector.footage_api_token_manager import FootageApiTokenManager

@pytest.mark.unit
class TestFootageApiWrapper():  # pylint: disable=too-few-public-methods
    """ Tests on Footage API Wrapper Component. """

    @mock.patch("urllib3.PoolManager")
    def test_correct_call_of_footage_api(
            self,
            mock_http_client):
        """ Test correct response from footage api. """
        # GIVEN
        mock_footage_api_token_manager = mock.Mock()
        mock_footage_api_token_manager.get_token = mock.Mock(return_value="this_is_a_super_token")
        footage_api_wrapper = FootageApiWrapper(
            footage_api_token_manager=mock_footage_api_token_manager,
            footage_api_url="http://example.local/{}")

        mock_http_client.return_value.request.return_value.status = 200

        # WHEN
        footage_api_wrapper.request_recorder("TRAINING", "test_device", 123, 1234)

        # THEN
        body = json.dumps({"from": "123", "to": "1234", "recorder": "TRAINING"})
        request_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer this_is_a_super_token"
        }

        mock_http_client.return_value.request.assert_called_once_with(
            "POST", "http://example.local/test_device", headers=request_headers, body=body)
