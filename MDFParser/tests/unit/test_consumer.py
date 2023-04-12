""" Unit tests for the Downloader class. """
import json
from unittest.mock import ANY, Mock
import pytest
from mdfparser.consumer import Consumer
from mdfparser.config import MdfParserConfig
from mdfparser.exceptions import InvalidMessage, InvalidFileNameException, HandlerTypeNotExist, InvalidMessage, NoProcessingSuccessfulException, FailToParseIMU

S3_PATH = "s3://bucket/key_metadata_full.json"


@pytest.mark.usefixtures("consumer", "sqs_raw_message", "container_services_mock", "s3_client", "config")
@pytest.mark.unit
class TestConsumer():
    """ Tests the Consumer class """

    @pytest.mark.parametrize("consume_return_val,sqs_raw_message",
                             [
                                 (
                                     Mock(),
                                     "input_imu_raw_message.json",
                                 ),
                                 (
                                     Mock(),
                                     "input_metadata_raw_message.json",
                                 ),
                                 (
                                     None,
                                     "input_metadata_raw_message.json",
                                 )
                             ],
                             indirect=["sqs_raw_message"],
                             ids=["test_consumer_1", "test_consumer_2", "test_consumer_3"]
                             )
    def test_consume_msg(
            self,
            consume_return_val: dict,
            sqs_raw_message: dict,
            consumer: Consumer,
            container_services_mock: Mock,
            config: MdfParserConfig) -> None:

        # WHEN
        result_to_json_mock = Mock()
        if isinstance(consume_return_val, Mock):
            consume_return_val.to_json = Mock(return_value=result_to_json_mock)

        container_services_mock.get_single_message_from_input_queue = Mock(return_value=sqs_raw_message)
        consumer.consume_msg = Mock(return_value=consume_return_val)

        # THEN
        consumer.run(Mock(side_effect=[True, False]))

        # ASSERT
        container_services_mock.get_single_message_from_input_queue.assert_any_call(ANY, config.input_queue)
        consumer.consume_msg.assert_called_once_with(ANY)
        if consume_return_val:
            container_services_mock.send_message.assert_called_once_with(
                ANY, config.metadata_output_queue, consume_return_val.to_json())
        else:
            container_services_mock.send_message.assert_not_called()

        container_services_mock.delete_message(ANY, sqs_raw_message["ReceiptHandle"], config.input_queue)
