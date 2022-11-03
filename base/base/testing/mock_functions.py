""" Module containing all mocks. """
import os
from unittest.mock import Mock, PropertyMock

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
    os.environ["CONFIG_S3"] = "test_bucket"
    service = ContainerServices("test_container", "alpha")
    service.load_config_vars = Mock(return_value=1)  # type: ignore
    service.get_sqs_queue_url = Mock(return_value="mock_test_url")  # type: ignore
    service.listen_to_input_queue = Mock(return_value={"Body": "Queue_message_body"})  # type: ignore
    service.delete_message = Mock(return_value=None)  # type: ignore
    service.update_message_visibility = Mock(return_value=None)  # type: ignore
    service.send_message = Mock(return_value=None)  # type: ignore
    service.get_db_connstring = Mock(return_value="mock_connection_string")  # type: ignore
    service.create_db_client = Mock(return_value=None)  # type: ignore
    service.get_message_body = Mock(return_value=None)  # type: ignore
    service.download_file = Mock(return_value="random_object")  # type: ignore
    service.upload_file = Mock(return_value=None)  # type: ignore
    service.update_pending_queue = Mock(return_value=[])  # type: ignore
    service.get_kinesis_clip = Mock(return_value="binary_data")  # type: ignore
    service.display_processed_msg = Mock(return_value=None)  # type: ignore

    type(service).ivs_api = PropertyMock(return_value={"port": "mock_port",  # type: ignore
                                                       "endpoint": "test_endpoint_mock",
                                                       "address": "ivs_hostname_mock"
                                                       })

    type(service).sqs_queues_list = PropertyMock(return_value=QUEUE_MOCK_LIST)  # type: ignore
    type(service).raw_s3 = PropertyMock(return_value="s3_mock")  # type: ignore
    type(service).anonymized_s3 = PropertyMock(return_value="anonimized_s3_mock")  # type: ignore

    return service
