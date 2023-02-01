""" Main Unit Test Module for anon_ivschain"""
from unittest.mock import patch, ANY, Mock
import pytest
from anon_ivschain.main import main, AWS_REGION, AWS_ENDPOINT
from anon_ivschain.main import CONTAINER_NAME, CALLBACK_ENDPOINT, MODE, API_PORT, CONTAINER_VERSION


@pytest.mark.unit
@patch("anon_ivschain.main.ContainerServices")
@patch("anon_ivschain.main.AWSServiceClients")
@patch("anon_ivschain.main.AnonymizeCallbackEndpointCreator")
@patch("anon_ivschain.main.BaseHandler")
@patch("anon_ivschain.main.boto3.client")
@patch("anon_ivschain.main.AnonymizePostProcessor")
@patch("anon_ivschain.main._logger")
def test_main(_logger, post_processor, boto3_client, base_handler, endpoint, _, container_services):
    """ main test function for anon_ivschain """
    # GIVEN

    _logger.info = Mock()
    handler_object = Mock()
    base_handler.return_value = handler_object
    handler_object.setup_and_run = Mock()

    endpoint.return_value = "MOCK_ENDPOINT"
    post_processor.return_value = "MOCK_POST_PROCESSOR"

    # WHEN
    main()

    # THEN
    _logger.info.assert_called()
    container_services.assert_called_once_with(CONTAINER_NAME, CONTAINER_VERSION)
    boto3_client.assert_any_call("s3", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT)
    boto3_client.assert_any_call("sqs", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT)
    endpoint.assert_called_once()
    base_handler.assert_called_once_with(CONTAINER_NAME, ANY, ANY, MODE, CALLBACK_ENDPOINT, "MOCK_ENDPOINT")
    handler_object.setup_and_run.assert_called_once_with(API_PORT, "MOCK_POST_PROCESSOR")
