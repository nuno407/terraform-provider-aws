"""Testing module for chc handler main function"""
import io
from typing import Tuple
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from flask import Blueprint
from flask import Flask
from flask.testing import FlaskClient

from basehandler.message_handler import InternalMessage
from chc_ivschain.callback_endpoint import CHCCallbackEndpointCreator

MOCK_ENDPOINT = "/mock_endpoint"


@pytest.mark.unit
class TestCHCCallbackEndpointCreator():
    """
    Class with tests for the chc callback endpoint
    """
    @pytest.fixture
    def client_endpoint(self) -> Tuple[FlaskClient, MagicMock]:
        """
        Test for client endpoint
        """
        # GIVEN
        endpoint = MOCK_ENDPOINT
        notifier = MagicMock()
        app = Flask(__name__)
        app.register_blueprint(
            CHCCallbackEndpointCreator.create(endpoint, notifier))

        # WHEN
        with app.test_client() as client:
            return client, notifier

    def test_blueprint_creation(self):
        """
        Test for blueprint creation
        """
        # WHEN
        blue_print = CHCCallbackEndpointCreator().create(MOCK_ENDPOINT, MagicMock())

        # THEN
        assert isinstance(blue_print, Blueprint)

    def test_output_405_get(self, client_endpoint: Tuple[FlaskClient, MagicMock]):
        """
        Test client endpoint for 405 response
        """
        # GIVEN
        response = client_endpoint[0].get(MOCK_ENDPOINT)

        # THEN
        assert response.status_code == 405

    def test_output_post_400(self, client_endpoint: Tuple[FlaskClient, MagicMock]):
        """
        Test client endpoint for 400 response
        """
        # GIVEN
        response = client_endpoint[0].post(MOCK_ENDPOINT)
        print(response)

        # THEN
        assert response.status_code == 400

    @patch("chc_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_1(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):
        """
        Test client endpoint for post output with mp4 path
        """
        # GIVEN
        form_field = {
            "metadata": (io.BytesIO(b"abcdef"), "test.jpg"),
            "uid": "random_uid",
            "path": "path_abc/a.b.mp4"
        }

        client_endpoint[1].container_services.anonymized_s3 = "mock_container"

        # WHEN
        response = client_endpoint[0].post(MOCK_ENDPOINT, data=form_field, content_type="multipart/form-data")

        # THEN
        assert response.status_code == 202
        assert thread.call_count == 1

        _, _args = thread.call_args
        _kwargs = _args["kwargs"]
        actual_internal_message = _kwargs["internal_message"]

        assert actual_internal_message.uid == form_field["uid"]
        assert actual_internal_message.status == InternalMessage.Status.PROCESSING_COMPLETED
        assert actual_internal_message.bucket == "mock_container"
        assert actual_internal_message.media_path is None
        assert actual_internal_message.meta_path == "path_abc/a.b_chc.json"

    @patch("chc_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_2(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):
        """
        Test client endpoint for post output with jpg path
        """
        # GIVEN
        form_field = {
            "metadata": (io.BytesIO(b"abcdef"), "test.jpg"),
            "uid": "random_uid",
            "path": "path_abc/a.b.jpg"
        }

        client_endpoint[1].container_services.anonymized_s3 = "mock_container"

        # WHEN
        response = client_endpoint[0].post(MOCK_ENDPOINT, data=form_field, content_type="multipart/form-data")

        # THEN
        assert response.status_code == 202
        assert thread.call_count == 1

        _, _args = thread.call_args
        _kwargs = _args["kwargs"]
        actual_internal_message = _kwargs["internal_message"]

        assert actual_internal_message.uid == form_field["uid"]
        assert actual_internal_message.status == InternalMessage.Status.PROCESSING_COMPLETED
        assert actual_internal_message.bucket == "mock_container"
        assert actual_internal_message.media_path is None
        assert actual_internal_message.meta_path == "path_abc/a.b_chc.json"

    def test_output_post_404_1(self, client_endpoint: Tuple[FlaskClient, MagicMock]):
        """
        Test client endpoint for 404 response
        """
        # GIVEN
        form_field = {
            "metadata": (io.BytesIO(b"abcdef"), "test.jpg"),
            "uid": "random_uid",
        }

        client_endpoint[1].container_services.anonymized_s3 = "mock_container"

        # WHEN
        response = client_endpoint[0].post(MOCK_ENDPOINT, data=form_field, content_type="multipart/form-data")

        # THEN
        assert response.status_code == 400
