"""Testing module for chc handler main function"""
from unittest.mock import patch, ANY, Mock
import pytest
from chc_ivschain import main


@pytest.mark.unit
@patch("chc_ivschain.main.ContainerServices")
@patch("chc_ivschain.main.CHCCallbackEndpointCreator")
@patch("chc_ivschain.main.BaseHandler")
@patch("chc_ivschain.main.boto3.client")
@patch("chc_ivschain.main._logger")
def test_main(
        _logger: Mock,
        boto3_client: Mock,
        base_handler: Mock,
        endpoint: Mock,
        container_services: Mock):
    """
    Test for chc handler main function
    """
    # GIVEN

    _logger.info = Mock()
    handler_object = Mock()
    base_handler.return_value = handler_object
    handler_object.setup_and_run = Mock()

    endpoint.return_value = "MOCK_ENDPOINT"

    # WHEN
    main.main()

    # THEN
    _logger.info.assert_called()
    container_services.assert_called_once_with(main.CONTAINER_NAME, main.CONTAINER_VERSION)
    boto3_client.assert_any_call("s3", region_name=main.AWS_REGION, endpoint_url=main.AWS_ENDPOINT)
    boto3_client.assert_any_call("sqs", region_name=main.AWS_REGION, endpoint_url=main.AWS_ENDPOINT)
    endpoint.assert_called_once()
    base_handler.assert_called_once_with(main.CONTAINER_NAME, ANY, ANY, main.MODE,
                                         main.CALLBACK_ENDPOINT, endpoint.return_value)
    handler_object.setup_and_run.assert_called_once_with(main.API_PORT)
