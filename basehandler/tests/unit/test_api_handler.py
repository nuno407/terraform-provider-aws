""" Test API Handler. """
from multiprocessing import Queue
from typing import Dict
from unittest.mock import Mock

import flask
import pytest
from flask import Blueprint
from pytest_mock import MockerFixture

from base.aws.shared_functions import AWSServiceClients
from base.testing.mock_functions import get_container_services_mock
from basehandler.api_handler import (APIHandler, OutputEndpointNotifier,
                                     OutputEndpointParameters)
from basehandler.message_handler import InternalMessage


class TestApiHandler():  # pylint: disable=missing-function-docstring,missing-class-docstring

    @staticmethod
    def _create_callback_blueprint(route_endpoint: str) -> Blueprint:
        anon_output_bp = Blueprint("anon_output_bp", __name__)

        @anon_output_bp.route(route_endpoint, methods=["POST"])
        def anonymize_output_handler() -> flask.Response:
            return flask.Response(status=200, response="upload video to storage")

        return anon_output_bp

    @pytest.fixture
    def client_api_handler(self, request: MockerFixture) -> APIHandler:

        args = {
            "consumer_name": "mock",
            "route_endpoint": "/mock",
            "run_message_handler_thread": True  # Boolean representing the function thread.is_alive()
        }

        # Allows to specify arguments in the parameters
        if hasattr(request, "param") and isinstance(request.param, dict):  # type: ignore
            if "consumer_name" in request.param:  # type: ignore
                args["consumer_name"] = request.param["consumer_name"]  # type: ignore
            if "run_message_handler_thread" in request.param:  # type: ignore
                args["run_message_handler_thread"] = request.param["run_message_handler_thread"]  # type: ignore
            if "route_endpoint" in request.param:  # type: ignore
                args["route_endpoint"] = request.param["route_endpoint"]  # type: ignore

        mock_output_notifier = Mock()
        mock_output_notifier.notify_error = Mock(return_value=None)

        # Create clients
        output_parameters = Mock()
        api_handler = APIHandler(
            output_parameters,
            TestApiHandler._create_callback_blueprint(route_endpoint=args["route_endpoint"]),  # type: ignore
            mock_output_notifier)

        return api_handler

    @pytest.fixture
    def client_output_notifier(self) -> OutputEndpointNotifier:

        # Create clients
        output_parameters = OutputEndpointParameters(
            get_container_services_mock(), "mock", AWSServiceClients(
                "mock_sqs", "mock_s3"), Queue())
        output_notifier = OutputEndpointNotifier(output_parameters)

        return output_notifier

    ################ START OF TESTS ####################

    def test_error_endpoint(self, client_api_handler: APIHandler):

        # GIVEN
        output_api = client_api_handler.create_routes()

        with output_api.test_client() as test_client:
            response = test_client.get("/no_exist_endpoint")

            # THEN
            assert response.status_code != 200

    def test_processing_error_endpoint(self, client_api_handler: APIHandler):
        # GIVEN
        output_api = client_api_handler.create_routes()

        with output_api.test_client() as test_client:
            response = test_client.post("/processingerror")

            # THEN
            assert response.status_code == 200

    def test_alive_thread_endpoint(self, client_api_handler: APIHandler):
        # GIVEN
        output_api = client_api_handler.create_routes()

        # WHEN
        with output_api.test_client() as test_client:
            response = test_client.get("/alive")

            # THEN
            assert response.status_code == 200

    @pytest.mark.parametrize("client_api_handler", [{"route_endpoint": "/mock_test"}], indirect=True)
    def test_new_endpoint(self, client_api_handler: APIHandler):

        # GIVEN
        output_api = client_api_handler.create_routes()

        # WHEN
        with output_api.test_client() as test_client:
            response = test_client.post("/mock_test")

            # THEN
            assert response.status_code == 200

    def test_upload_and_notify1(self, client_output_notifier: OutputEndpointNotifier):

        # GIVEN
        args: Dict[str, object] = {
            "chunk": "aysdtauystduastd",
            "path": "path/agcfhgcf/abc.txt",
            "internal_message": InternalMessage(InternalMessage.Status.PROCESSING_COMPLETED)
        }

        # WHEN
        client_output_notifier.upload_and_notify(**args)  # type: ignore

        # THEN
        assert client_output_notifier.internal_queue.qsize() == 1

        queue_msg = client_output_notifier.internal_queue.get()
        assert queue_msg.status == InternalMessage.Status.OK

        upload_args, _ = client_output_notifier.container_services.upload_file.call_args
        assert args["chunk"] in upload_args
        assert args["path"] in upload_args

    def test_upload_and_notify2(self, client_output_notifier: OutputEndpointNotifier):

        # GIVEN
        args: Dict[str, object] = {
            "chunk": "aysdtauystduastd",
            "path": "path/agcfhgcf/abc.txt",
            "internal_message": InternalMessage(InternalMessage.Status.PROCESSING_COMPLETED)
        }

        # WHEN
        client_output_notifier.upload_and_notify(**args)  # type: ignore

        # THEN
        assert client_output_notifier.internal_queue.qsize() == 1

        queue_msg = client_output_notifier.internal_queue.get()
        assert queue_msg.status == InternalMessage.Status.OK

        upload_args, _ = client_output_notifier.container_services.upload_file.call_args
        assert args["chunk"] in upload_args
        assert args["path"] in upload_args

    def test_upload_and_notify_error(self, client_output_notifier: OutputEndpointNotifier):

        # GIVEN
        args: Dict[str, object] = {
            "chunk": "aysdtauystduastd",
            "path": "path/agcfhgcf/abc.txt",
            "internal_message": "not_parsed"
        }

        # WHEN-THEN
        with pytest.raises(TypeError):
            client_output_notifier.upload_and_notify(**args)  # type: ignore
            client_output_notifier.container_services.upload_file.assert_not_called()
            assert client_output_notifier.internal_queue.qsize() == 0

    def test_notify_error(self, client_output_notifier: OutputEndpointNotifier):

        # WHEN
        client_output_notifier.notify_error()

        # THEN
        assert client_output_notifier.internal_queue.qsize() == 1

        queue_msg = client_output_notifier.internal_queue.get()
        assert queue_msg.status == InternalMessage.Status.ERROR
