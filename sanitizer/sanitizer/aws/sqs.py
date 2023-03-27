""" AWS SQS controller module. """
from datetime import datetime
from logging import Logger
from typing import Optional

from expiringdict import ExpiringDict
from kink import inject
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

from sanitizer.config import SanitizerConfig
from sanitizer.exceptions import InitializationError
from sanitizer.model import SQSMessage


@inject
class AWSSQSController:
    """ SQS Message Controller. """
    def __init__(self, config: SanitizerConfig, sqs_client: SQSClient):
        self.__config = config
        self.__sqs_client = sqs_client
        self.__message_receive_times: ExpiringDict[str, datetime] = ExpiringDict(
            max_len=1000, max_age_seconds=50400)

    def get_queue_url(self) -> str:
        """Get queue url.

        This should happen only once during initialization of the service.

        Return:
            str: queue url
        """
        response = self.__sqs_client.get_queue_url(
            QueueName=self.__config.input_queue)
        if not response or ("QueueUrl" not in response):
            raise InitializationError("Invalid get queue url reponse")
        return response["QueueUrl"]

    def get_message(self, queue_url: str) -> Optional[MessageTypeDef]:
        """Get SQS queue message

        Args:
            queue_url (str): the SQS queue url to retrieve the message

        Returns:
            Optional[str]: raw message if any in response or None
        """
        message = None
        response = self.__sqs_client.receive_message(
            QueueUrl=queue_url,
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

        if "Messages" in response and len(response["Messages"]) > 0:
            message = response["Messages"][0]
            self.__message_receive_times[message["ReceiptHandle"]] = datetime.now(
            )
        return message

    def delete_message(self, input_queue_url: str, sqs_message: SQSMessage) -> None:
        """Deletes message from SQS queue

        Args:
            input_queue_url (str): the SQS queue url
            sqs_message (SQSMessage): the SQS queue to be deleted
        """
        self.__message_receive_times.pop(sqs_message.receipt_handle, None)

        self.__sqs_client.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=sqs_message.receipt_handle
        )
