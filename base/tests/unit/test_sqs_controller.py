# type: ignore
"""Unit tests SQS controller."""
from unittest.mock import Mock

import pytest
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import (TWELVE_HOURS_IN_SECONDS, InitializationError,
                          SQSController)

CONTAINER_NAME = "test-container"


@pytest.mark.unit
class TestSQSController():
    """Test SQS controller."""

    @pytest.fixture
    def input_queue_name(self) -> str:
        return "test-input"

    @pytest.fixture
    def sqs_message(self) -> MessageTypeDef:
        return {
            "MessageId": "foobar",
            "ReceiptHandle": "foobar-receipt",
            "Body": "foobar-body"
        }

    @pytest.fixture
    def sqs_client_mock(self):
        sqs_client_mock = Mock()
        sqs_client_mock.get_queue_url = Mock(return_value={
            "QueueUrl": "foobar-url"
        })
        return sqs_client_mock

    def test_get_queue_url_success(self, input_queue_name: str, sqs_client_mock: Mock):
        # WHEN
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

        # THEN
        assert message_controller._SQSController__queue_url == "foobar-url"
        sqs_client_mock.get_queue_url.assert_called_once()

    def test_get_queue_url_fail(self, input_queue_name: str, sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.get_queue_url = Mock(
            side_effect=InitializationError("error"))

        # WHEN THEN
        with pytest.raises(InitializationError):
            SQSController(
                default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

    def test_delete_message_success(
            self,
            input_queue_name: str,
            sqs_message: MessageTypeDef,
            sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.delete_message = Mock()
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

        # WHEN
        message_controller.delete_message(sqs_message)

        # THEN
        sqs_client_mock.delete_message.assert_called_once_with(
            QueueUrl="foobar-url", ReceiptHandle="foobar-receipt")

    def test_get_message_success(self, input_queue_name: str, sqs_client_mock: Mock):
        # GIVEN
        given_message = {"title": "my-message",
                         "ReceiptHandle": "foobar-receipt"}
        sqs_client_mock.receive_message = Mock(
            return_value={"Messages": [given_message]})
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

        # WHEN
        got_message = message_controller.get_message()

        # THEN
        sqs_client_mock.receive_message.assert_called_once_with(
            QueueUrl="foobar-url",
            AttributeNames=[
                "SentTimestamp",
                "ApproximateReceiveCount"
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                "All"
            ],
            WaitTimeSeconds=20
        )
        assert got_message == given_message

    def test_get_message_empty(self, input_queue_name: str, sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.receive_message = Mock(return_value={"Messages": []})
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

        # WHEN
        got_message = message_controller.get_message()

        # THEN
        sqs_client_mock.receive_message.assert_called_once_with(
            QueueUrl="foobar-url",
            AttributeNames=[
                "SentTimestamp",
                "ApproximateReceiveCount"
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                "All"
            ],
            WaitTimeSeconds=20
        )
        assert got_message is None

    def test_increase_visibility_timeout_and_handle_exceptions(
            self, input_queue_name: str, sqs_message: MessageTypeDef,
            sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.change_message_visibility = Mock()
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)

        # WHEN
        message_controller.try_update_message_visibility_timeout(
            sqs_message, TWELVE_HOURS_IN_SECONDS)

        # THEN
        sqs_client_mock.change_message_visibility.assert_called_once_with(
            QueueUrl="foobar-url",
            ReceiptHandle=sqs_message["ReceiptHandle"],
            VisibilityTimeout=TWELVE_HOURS_IN_SECONDS - 1
        )

    def test_send_message_to_own_queue(self, input_queue_name: str, sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.send_message = Mock()

        # WHEN
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        message_controller.send_message("Hello World", CONTAINER_NAME)

        # THEN
        sqs_client_mock.send_message.assert_called_once_with(
            QueueUrl="foobar-url",
            MessageBody="Hello World",
            MessageAttributes={
                "SourceContainer": {
                    "DataType": "String",
                    "StringValue": CONTAINER_NAME
                }
            }
        )

    def test_send_message_to_other_queue(self, input_queue_name: str, sqs_client_mock: Mock):
        # GIVEN
        sqs_client_mock.send_message = Mock()

        # WHEN
        message_controller = SQSController(
            default_sqs_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        message_controller.send_message("Hello World", CONTAINER_NAME, "other-queue")

        # THEN
        sqs_client_mock.get_queue_url.assert_called_with(QueueName="other-queue")
        sqs_client_mock.send_message.assert_called_once_with(
            QueueUrl="foobar-url",
            MessageBody="Hello World",
            MessageAttributes={
                "SourceContainer": {
                    "DataType": "String",
                    "StringValue": CONTAINER_NAME
                }
            }
        )
