""" Test Entrypoint Module."""
from unittest import mock
from unittest.mock import Mock

import flask
from flask import Blueprint

from base.aws.shared_functions import AWSServiceClients
from base.testing.mock_functions import get_container_services_mock
from basehandler.api_handler import OutputEndpointNotifier
from basehandler.entrypoint import BaseHandler, CallbackBlueprintCreator

# pylint: disable=missing-function-docstring,missing-class-docstring,too-few-public-methods


class MockCallbackEndpointCreator(CallbackBlueprintCreator):
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        chc_output_bp = Blueprint("chc_output_bp", __name__)

        @chc_output_bp.route(route_endpoint, methods=["POST"])
        def chc_output_handler() -> flask.Response:
            return flask.Response(status=200, response="uploading video to storage")

        return chc_output_bp


def test_basehandler_creation():
    base_handler = BaseHandler("MOCK",
                               get_container_services_mock(),
                               AWSServiceClients("sqs", "client"),
                               "chc",
                               "/callback",
                               MockCallbackEndpointCreator())

    assert base_handler is not None


@mock.patch("basehandler.entrypoint.APIHandler")
@mock.patch("basehandler.entrypoint.Process")
@mock.patch("basehandler.entrypoint.MessageHandler")
@mock.patch("basehandler.message_handler")
def test_basehandler_setup_and_run(
        message_handler_module_mock: Mock,
        message_handler_mock: Mock,
        process_mock: Mock,
        api_handler_mock: Mock):
    mock_object_message = Mock()
    mock_object_process = Mock()
    mock_object_api_handler = Mock()

    mock_object_output_api_handler = Mock()
    message_handler_module_mock = Mock()
    message_handler_module_mock.wait_for_featurechain = Mock()

    mock_object_message.start = Mock(return_value=True)
    mock_object_process.start = Mock(return_value=True)
    mock_object_api_handler.create_routes = Mock(
        return_value=mock_object_output_api_handler)

    api_handler_mock.return_value = mock_object_api_handler
    process_mock.return_value = mock_object_process
    message_handler_mock.return_value = mock_object_message
    port = "2000"

    base_handler = BaseHandler("MOCK",
                               get_container_services_mock(),
                               AWSServiceClients("sqs", "client"),
                               "chc",
                               "/callback",
                               MockCallbackEndpointCreator())

    base_handler.setup_and_run(port)
    process_mock.assert_called()
    mock_object_process.start.assert_called()
