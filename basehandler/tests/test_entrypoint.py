import os
from unittest.mock import Mock, PropertyMock
from unittest import mock

from base.aws.shared_functions import AWSServiceClients
from basehandler.entrypoint import BaseHandler, CallbackBlueprintCreator
from basehandler.api_handler import OutputEndpointNotifier
from base.testing.mock_functions import get_container_services_mock
from flask import Blueprint
import flask

class MockCallbackEndpointCreator(CallbackBlueprintCreator):
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        chc_output_bp = Blueprint('chc_output_bp', __name__)

        @chc_output_bp.route(route_endpoint, methods=["POST"])
        def chc_output_handler() -> flask.Response:
            return flask.Response(status=200, response='uploading video to storage')

        return chc_output_bp


def test_basehandler_creation():
    base_handler = BaseHandler("MOCK",
                           get_container_services_mock(),
                           AWSServiceClients("sqs","client"),
                           'chc',
                           "/callback",
                           MockCallbackEndpointCreator())

    assert base_handler != None

@mock.patch("basehandler.entrypoint.APIHandler")
@mock.patch("basehandler.entrypoint.threading.Thread")
@mock.patch("basehandler.entrypoint.MessageHandler")
def test_basehandler_setup_and_run(message_handler_mock: Mock, thread_mock: Mock, api_handler_mock: Mock):

    #GIVEN
    mock_object_message = Mock()
    mock_object_thread = Mock()
    mock_object_api_handler = Mock()

    mock_object_output_api_handler = Mock()

    mock_object_message.start = Mock(return_value=True)
    mock_object_thread.start = Mock(return_value=True)
    mock_object_api_handler.create_routes = Mock(return_value=mock_object_output_api_handler)

    mock_object_output_api_handler.run = Mock(return_value=True)

    api_handler_mock.return_value = mock_object_api_handler
    thread_mock.return_value = mock_object_thread
    message_handler_mock.return_value = mock_object_message
    port = 2000


    # WHEN
    base_handler = BaseHandler("MOCK",
                           get_container_services_mock(),
                           AWSServiceClients("sqs","client"),
                           'chc',
                           "/callback",
                           MockCallbackEndpointCreator())

    # THEN
    base_handler.setup_and_run(port)
    thread_mock.assert_called()
    mock_object_thread.start.assert_called()
    mock_object_output_api_handler.run.assert_called()
