""" FootageApiWrapper Tests. """
import json
from datetime import datetime, timedelta, timezone
from unittest import mock
import pytest
from selector.footage_api_wrapper import FootageApiWrapper
from base.model.artifacts import RecorderType

dummy_date = datetime(year=2023, month=1, day=10, hour=1, tzinfo=timezone.utc)


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
        footage_api_response = {"footageId": "a_dummy_footage_id"}
        mock_http_client.return_value.request.return_value.data = bytes(json.dumps(footage_api_response), "utf8")

        start_date = dummy_date
        end_date = dummy_date + timedelta(minutes=2)

        # WHEN
        footage_id = footage_api_wrapper.request_recorder(
            RecorderType.TRAINING,
            "test_device",
            start_date,
            end_date)

        # THEN
        assert footage_id == footage_api_response["footageId"]
        body = json.dumps({"from": "1673312400000", "to": "1673312520000", "recorder": "TRAINING"})
        request_headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer this_is_a_super_token"
        }

        mock_http_client.return_value.request.assert_called_once_with(
            "POST", "http://example.local/test_device", headers=request_headers, body=body)
