""" API Token Manager Tests. """
import base64
from datetime import datetime
import json
from unittest import mock
import pytest
from selector.footage_api_token_manager import FootageApiTokenManager, InvalidConfiguration


@pytest.mark.unit
class TestFootageApiTokenManager():
    """ Tests on FootageApiTokenManager. """

    @pytest.mark.parametrize("token_endpoint,client_id,client_secret",
                             [("test", "", ""), ("test", "test", ""), ("test", "", "test")])
    def test_invalid_configuration(self, token_endpoint, client_id, client_secret):
        """ Tests if FootageApiTokenManager can be initialized with invalid parameters"""
        # WHEN / THEN
        with pytest.raises(InvalidConfiguration):
            FootageApiTokenManager(token_endpoint=token_endpoint, client_id=client_id, client_secret=client_secret)

    def test_usage_of_cached_token(self):
        """ Test usage of token in cache. """
        # GIVEN
        footage_api_token_manager = FootageApiTokenManager(
            token_endpoint="test", client_id="test", client_secret="test")
        footage_api_token_manager.set_token(
            access_token="this_is_a_super_token",
            token_expires=int(
                datetime(
                    2100,
                    1,
                    1).timestamp()))

        # WHEN
        token = footage_api_token_manager.get_token()

        # THEN
        assert token == "this_is_a_super_token"

    @mock.patch("urllib3.PoolManager")
    def test_correct_call_of_footage_api_auth(self, mock_http_client):
        """ Test correct response from footage api auth. """
        # GIVEN
        footage_api_token_manager = FootageApiTokenManager(
            token_endpoint="http://example.local", client_id="test_client_id", client_secret="test_client_secret")

        response_body = json.dumps({
            "access_token": "bla",
            "expires_in": int(datetime(2100, 1, 1).timestamp())
        }).encode("utf-8")

        mock_http_client.return_value.request.return_value.status = 200
        mock_http_client.return_value.request.return_value.data = response_body

        # WHEN
        token = footage_api_token_manager.get_token()

        # THEN
        assert token == "bla"
        request_encoded_body = "grant_type=client_credentials"
        basic_token = base64.b64encode("test_client_id:test_client_secret".encode("utf-8")).decode("utf-8")
        request_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_token}"
        }

        mock_http_client.return_value.request.assert_called_once_with(
            "POST", "http://example.local", headers=request_headers, body=request_encoded_body)

    @mock.patch("urllib3.PoolManager")
    def test_incorrect_request_body_on_footage_api_call_auth(self, mock_http_client):
        """ Test incorrect response from footage api auth. """
        # GIVEN
        footage_api_token_manager = FootageApiTokenManager(
            token_endpoint="http://example.local", client_id="test_client_id", client_secret="test_client_secret")

        mock_http_client.return_value.request.return_value.status = 400

        # WHEN
        token = footage_api_token_manager.get_token()

        # THEN
        assert token is None

        mock_http_client.return_value.request.assert_called_once()

    @mock.patch("urllib3.PoolManager")
    def test_incorrect_response_body_on_footage_api_call_auth(self, mock_http_client):
        """ Test incorrect response from footage api auth. """
        # GIVEN
        footage_api_token_manager = FootageApiTokenManager(
            token_endpoint="http://example.local", client_id="test_client_id", client_secret="test_client_secret")

        mock_http_client.return_value.request.return_value.status = 200
        mock_http_client.return_value.request.return_value.data = b"invalid json"

        # WHEN
        token = footage_api_token_manager.get_token()

        # THEN
        assert token is None
