import queue
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest

from flask import Blueprint
import flask

from baseaws.shared_functions import AWSServiceClients, ContainerServices
from basehandler.api_handler import APIHandler, OutputEndpointParameters, OutputEndpointNotifier
from baseaws.mock_functions import get_container_services_mock, QUEUE_MOCK_LIST
from pytest_mock import MockerFixture

def create_callback_blueprint(route_endpoint: str) -> Blueprint:
    anon_output_bp = Blueprint('anon_output_bp', __name__)
    @anon_output_bp.route(route_endpoint, methods=["POST"])
    
    def anonymize_output_handler() -> flask.Response:
        return flask.Response(status=200, response='upload video to storage')

    return anon_output_bp

class TestApiHandler():
    
    @pytest.fixture
    def client_api_handler(self, request: MockerFixture) -> APIHandler:
    
        args = {
                "consumer_name" : "mock",
                "route_endpoint" : "/mock",
                "run_message_handler_thread" : True #Boolean representing the function thread.is_alive()
        }
    
        # Allows to specify arguments in the parameters
        if hasattr(request, "param") and isinstance(request.param, dict):
            if "consumer_name" in request.param:
                args['consumer_name'] = request.param['consumer_name']
            if "run_message_handler_thread" in request.param:
                args['run_message_handler_thread'] = request.param['run_message_handler_thread']
            if "route_endpoint" in request.param:
                args['route_endpoint'] = request.param['route_endpoint']
    
        # Mock Thread
        mock_object = MagicMock()
        mock_object.is_alive = Mock(return_value=args['run_message_handler_thread'])
    
        # Create clients
        output_parameters = Mock()
        output_notifier = Mock()
        api_handler = APIHandler(output_parameters,
            create_callback_blueprint(args['route_endpoint']), mock_object)
    
        return api_handler
    
    
    @pytest.fixture
    def client_output_notifier(request: MockerFixture) -> OutputEndpointNotifier:
    
        # Create clients
        output_parameters = OutputEndpointParameters(get_container_services_mock(),"mock",
            AWSServiceClients("mock_sqs","mock_s3"), queue.Queue())
        output_notifier = OutputEndpointNotifier(output_parameters)
    
        return output_notifier
    
    
    ################ START OF TESTS ####################
    def test_error_endpoint(self, client_api_handler: APIHandler):
        
        # GIVEN
        output_api = client_api_handler.create_routes()
    
        with output_api.test_client() as c:
            response = c.get("/no_exist_endpoint")
    
            # THEN
            assert response.status_code != 200
    
    
    def test_alive_thread_endpoint(self, client_api_handler: APIHandler):
        # GIVEN
        output_api = client_api_handler.create_routes()
    
        # WHEN
        with output_api.test_client() as c:
            response = c.get("/alive")
    
            # THEN
            assert response.status_code == 200
    
    
    @pytest.mark.parametrize('client_api_handler', [dict(run_message_handler_thread=False)], indirect=True)
    def test_dead_thread_endpoint(self, client_api_handler: APIHandler):
        
        # GIVEN
        output_api = client_api_handler.create_routes()
    
        # WHEN
        with output_api.test_client() as c:
            response = c.get("/alive")
    
            # THEN
            print(response.data)
            assert response.status_code == 500
            assert response.data == b'Message handler thread error'
    
    @pytest.mark.parametrize('client_api_handler', [dict(route_endpoint="/mock_test")], indirect=True)
    def test_new_endpoint(self, client_api_handler: APIHandler):
        
        # GIVEN
        output_api = client_api_handler.create_routes()
    
        # WHEN
        with output_api.test_client() as c:
            response = c.post("/mock_test")
    
            # THEN
            assert response.status_code == 200
    
    
    def test_upload_and_notify1(self, client_output_notifier : OutputEndpointNotifier):
    
        # GIVEN
        args = {
            "chunk" : "aysdtauystduastd",
            "path" : "path/agcfhgcf/abc.txt",
            "msg_body" : {"The message" :"abc"}
        }

        # WHEN
        client_output_notifier.upload_and_notify(**args)
    
        # THEN
        assert client_output_notifier.internal_queue.qsize() == 1
        assert client_output_notifier.internal_queue.get(0) == args['msg_body']
    
        upload_args, _ = client_output_notifier.container_services.upload_file.call_args
        assert args["chunk"] in upload_args
        assert args["path"] in upload_args
    
    def test_upload_and_notify2(self, client_output_notifier : OutputEndpointNotifier):
    
        # GIVEN
        args = {
            "chunk" : "aysdtauystduastd",
            "path" : "path/agcfhgcf/abc.txt",
            "msg_body" : ["The message"]
        }
        
        # WHEN
        client_output_notifier.upload_and_notify(**args)
    
        # THEN
        assert client_output_notifier.internal_queue.qsize() == 1
        assert client_output_notifier.internal_queue.get() == args['msg_body']
    
        upload_args, _ = client_output_notifier.container_services.upload_file.call_args
        assert args["chunk"] in upload_args
        assert args["path"] in upload_args
    
    def test_upload_and_notify_error(self, client_output_notifier : OutputEndpointNotifier):
    
        # GIVEN
        args = {
            "chunk" : "aysdtauystduastd",
            "path" : "path/agcfhgcf/abc.txt",
            "msg_body" : "not_parsed"
        }

        # WHEN-THEN
        with pytest.raises(TypeError):
            client_output_notifier.upload_and_notify(**args)
            client_output_notifier.container_services.upload_file.assert_not_called()
            assert client_output_notifier.internal_queue.qsize() == 0
    