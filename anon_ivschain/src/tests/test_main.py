from unittest.mock import patch, ANY, Mock
import main


@patch('main.ContainerServices')
@patch('main.AWSServiceClients')
@patch('main.AnonymizeCallbackEndpointCreator')
@patch('main.BaseHandler')
@patch('main.boto3.client')
@patch('main.AnonymizePostProcessor')
@patch('main._logger')
def test_main(_logger, post_processor, boto3_client, base_handler, endpoint, _, container_services):
    # GIVEN

    _logger.info = Mock()
    handler_object = Mock()
    base_handler.return_value = handler_object
    handler_object.setup_and_run = Mock()

    endpoint.return_value = "MOCK_ENDPOINT"
    post_processor.return_value = "MOCK_POST_PROCESSOR"

    # WHEN
    main.main()

    # THEN
    _logger.info.assert_any_call(ANY)
    container_services.assert_called_once_with(main.CONTAINER_NAME, main.CONTAINER_VERSION)
    boto3_client.assert_any_call('s3', region_name=main.AWS_REGION, endpoint_url=main.AWS_ENDPOINT)
    boto3_client.assert_any_call('sqs', region_name=main.AWS_REGION, endpoint_url=main.AWS_ENDPOINT)
    endpoint.assert_called_once()
    base_handler.assert_called_once_with(main.CONTAINER_NAME, ANY, ANY, main.MODE, main.CALLBACK_ENDPOINT, "MOCK_ENDPOINT")
    handler_object.setup_and_run.assert_called_once_with(main.API_PORT, "MOCK_POST_PROCESSOR")
