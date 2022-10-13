import io
from typing import Tuple
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from flask import Blueprint
from flask import Flask
from flask.testing import FlaskClient

from anon_ivschain.callback_endpoint import AnonymizeCallbackEndpointCreator
from basehandler.message_handler import InternalMessage
from basehandler.message_handler import OperationalMessage

MOCK_ENDPOINT = "/mock_endpoint"


@pytest.mark.unit
class TestAnonymizeCallbackEndpointCreator():
    @pytest.fixture
    def client_endpoint(self) -> Tuple[FlaskClient, MagicMock]:
        # GIVEN
        endpoint = MOCK_ENDPOINT
        notifier = MagicMock()
        app = Flask(__name__)
        app.register_blueprint(
            AnonymizeCallbackEndpointCreator.create(endpoint, notifier))

        # WHEN
        with app.test_client() as c:
            return c, notifier

    def test_blueprint_creation(self):
        # GIVEN-WHEN
        blue_print = AnonymizeCallbackEndpointCreator().create(MOCK_ENDPOINT, MagicMock())

        # THEN
        assert type(blue_print) == Blueprint

    @patch("anon_ivschain.callback_endpoint.threading.Thread")
    def test_output_405_get(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):

        # GIVEN
        response = client_endpoint[0].get(MOCK_ENDPOINT)

        # THEN
        assert response.status_code == 405

    @patch("anon_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_400(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):

        # GIVEN
        response = client_endpoint[0].post(MOCK_ENDPOINT)
        print(response)

        # THEN
        assert response.status_code == 400

    @patch("anon_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_1(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):

        # GIVEN
        form_field = {
            'file': (io.BytesIO(b"abcdef"), 'test.mp4'),
            'uid': 'random_uid',
            'path': 'path_abc/a.b.mp4'
        }

        client_endpoint[1].container_services.anonymized_s3 = 'mock_container'

        # WHEN
        response = client_endpoint[0].post(
            MOCK_ENDPOINT, data=form_field, content_type='multipart/form-data')

        # THEN
        assert response.status_code == 202
        assert thread.call_count == 1

        _, _args = thread.call_args
        _kwargs = _args['kwargs']
        actual_internal_message = _kwargs['internal_message']

        assert _kwargs['chunk'] == b"abcdef"
        assert _kwargs['path'] == form_field['path'][:-4] + "_anonymized.mp4"
        assert actual_internal_message.uid == form_field['uid']
        assert actual_internal_message.status == OperationalMessage.Status.PROCESSING_COMPLETED
        assert actual_internal_message.bucket == 'mock_container'
        assert actual_internal_message.input_media == form_field['path']
        assert actual_internal_message.media_path == 'path_abc/a.b_anonymized.mp4'

    @patch("anon_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_2(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):

        # GIVEN
        form_field = {
            'file': (io.BytesIO(b"abcdef"), 'test.jpg'),
            'uid': 'random_uid',
            'path': 'path_abc/a.b_anonymize.jpg'
        }

        client_endpoint[1].container_services.anonymized_s3 = 'mock_container'

        expected_msg_body = {
            'uid': form_field['uid'],
            'status': 'processing completed',
            'bucket': 'mock_container',
            'media_path': 'path_abc/a.b_anonymize_anonymized.jpg',
            'meta_path': "-",
        }

        # WHEN
        response = client_endpoint[0].post(
            MOCK_ENDPOINT, data=form_field, content_type='multipart/form-data')

        # THEN
        assert response.status_code == 202
        assert thread.call_count == 1

        _, _args = thread.call_args
        _kwargs = _args['kwargs']
        actual_internal_message = _kwargs['internal_message']

        assert _kwargs['chunk'] == b"abcdef"
        assert _kwargs['path'] == expected_msg_body['media_path']
        assert actual_internal_message.uid == form_field['uid']
        assert actual_internal_message.status == InternalMessage.Status.PROCESSING_COMPLETED
        assert actual_internal_message.bucket == 'mock_container'
        assert actual_internal_message.media_path == 'path_abc/a.b_anonymize_anonymized.jpg'

    @patch("anon_ivschain.callback_endpoint.threading.Thread")
    def test_output_post_404_1(self, thread: Mock, client_endpoint: Tuple[FlaskClient, MagicMock]):

        # GIVEN
        form_field = {
            'file': (io.BytesIO(b"abcdef"), 'test.jpg'),
            'uid': 'random_uid',
        }

        client_endpoint[1].container_services.anonymized_s3 = 'mock_container'

        # WHEN
        response = client_endpoint[0].post(
            MOCK_ENDPOINT, data=form_field, content_type='multipart/form-data')

        # THEN
        assert response.status_code == 400
