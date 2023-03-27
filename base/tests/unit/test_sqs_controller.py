"""Unit tests SQS controller."""
from datetime import datetime
from unittest.mock import MagicMock, Mock

import botocore.exceptions
import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.aws.sqs import (TWELVE_HOURS_IN_SECONDS, InitializationError,
                          SQSController)


@pytest.mark.unit
class TestSQSController():
    """Test SQS controller."""

    @pytest.fixture
    def input_queue_name(self) -> str:
        return "test-input"

    @pytest.fixture
    def sqs_message(self) -> SQSMessage:
        return SQSMessage(
            message_id="foobar",
            receipt_handle="foobar-receipt",
            body={},
            timestamp=datetime.min,
            attributes=MessageAttributes(
                tenant="test",
                device_id="test-dev"))

    def test_get_queue_url_success(self, input_queue_name: str):
        sqs_client_mock = Mock()
        sqs_client_mock.get_queue_url = Mock(return_value={
            "QueueUrl": "foobar"
        })
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        queue_url = message_controller.get_queue_url()
        assert queue_url == "foobar"
        sqs_client_mock.get_queue_url.assert_called_once()

    def test_get_queue_url_fail(self, input_queue_name: str):
        sqs_client_mock = Mock()
        sqs_client_mock.get_queue_url = Mock(
            side_effect=InitializationError("error"))
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        with pytest.raises(InitializationError):
            message_controller.get_queue_url()

    def test_delete_message_success(self, input_queue_name: str, sqs_message: SQSMessage):
        sqs_client_mock = Mock()
        sqs_client_mock.delete_message = Mock()
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        message_controller.delete_message("foobar-url", sqs_message)
        sqs_client_mock.delete_message.assert_called_once_with(
            QueueUrl="foobar-url", ReceiptHandle="foobar-receipt")

    def test_get_message_success(self, input_queue_name: str):
        sqs_client_mock = Mock()
        given_message = {"title": "my-message",
                         "ReceiptHandle": "foobar-receipt"}
        sqs_client_mock.receive_message = Mock(
            return_value={"Messages": [given_message]})
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        got_message = message_controller.get_message("foobar-url")
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

    def test_get_message_empty(self, input_queue_name: str):
        sqs_client_mock = Mock()
        sqs_client_mock.receive_message = Mock(return_value={"Messages": []})
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        got_message = message_controller.get_message("foobar-url")
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
            self, input_queue_name: str, sqs_message: SQSMessage):
        sqs_client_mock = Mock()
        sqs_client_mock.change_message_visibility = Mock()
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        message_controller.try_update_message_visibility_timeout(
            "foobar-url", sqs_message, TWELVE_HOURS_IN_SECONDS)
        sqs_client_mock.change_message_visibility.assert_called_once_with(
            QueueUrl="foobar-url",
            ReceiptHandle=sqs_message.receipt_handle,
            VisibilityTimeout=TWELVE_HOURS_IN_SECONDS
        )

    def test_increase_visibility_timeout_and_handle_exceptions2(
            self, input_queue_name: str, sqs_message: SQSMessage):
        sqs_client_mock = Mock()
        sqs_client_mock.change_message_visibility = Mock(
            side_effect=botocore.exceptions.ClientError(
                error_response=MagicMock(), operation_name=MagicMock()))
        message_controller = SQSController(
            input_queue_name=input_queue_name, sqs_client=sqs_client_mock)
        message_controller.try_update_message_visibility_timeout(
            "foobar-url", sqs_message, TWELVE_HOURS_IN_SECONDS)
