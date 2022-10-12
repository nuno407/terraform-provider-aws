import os
from unittest.mock import Mock
from unittest.mock import PropertyMock

from base.aws.container_services import ContainerServices

QUEUE_MOCK_LIST = {
    "Anonymize": "mock_queue_anonymize",
    "CHC": "mock_queue_chc",
    "new_algo": "new_algo_queue_mock",
    "Metadata": "mock_metadata_queue"
}


def get_container_services_mock() -> ContainerServices:
    """
    Returns a ContainerServices class with every element mocked
    """

    # Needed by the constructor of the ContainerServices
    os.environ['CONFIG_S3'] = "test_bucket"
    service = ContainerServices('test_container', 'alpha')
    service.load_config_vars = Mock(return_value=1)
    service.get_sqs_queue_url = Mock(return_value="mock_test_url")
    service.listen_to_input_queue = Mock(
        return_value={'Body': 'Queue_message_body'})
    service.delete_message = Mock(return_value=None)
    service.update_message_visibility = Mock(return_value=None)
    service.send_message = Mock(return_value=None)
    service.get_db_connstring = Mock(return_value="mock_connection_string")
    service.create_db_client = Mock(return_value=None)
    service.get_message_body = Mock(return_value=None)
    service.download_file = Mock(return_value="random_object")
    service.upload_file = Mock(return_value=None)
    service.update_pending_queue = Mock(return_value=[])
    service.get_kinesis_clip = Mock(return_value="binary_data")
    service.display_processed_msg = Mock(return_value=None)

    type(service).ivs_api = PropertyMock(return_value={"port": "mock_port",
                                                       "endpoint": "test_endpoint_mock",
                                                       "address": "ivs_hostname_mock"
                                                       })

    type(service).sqs_queues_list = PropertyMock(return_value=QUEUE_MOCK_LIST)
    type(service).raw_s3 = PropertyMock(return_value="s3_mock")
    type(service).anonymized_s3 = PropertyMock(
        return_value="anonimized_s3_mock")

    return service
