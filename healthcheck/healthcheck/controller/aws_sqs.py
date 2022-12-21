"""AWS SQS controller."""
import logging
from typing import Optional

import botocore.exceptions
from aws_error_utils import errors as aws_errors
from kink import inject
from mypy_boto3_sqs import SQSClient

from healthcheck.constants import TWELVE_HOURS_IN_SECONDS
from healthcheck.exceptions import InitializationError
from healthcheck.model import SQSMessage

_logger: logging.Logger = logging.getLogger(__name__)


@inject
class SQSMessageController():
    """SQS message controller."""
    def __init__(self,
                 sqs_client: SQSClient):
        self.__sqs_client = sqs_client

    def get_queue_url(self) -> str:
        """Get queue url.

        This should happen only once during initialization of the service.

        Return:
            str: queue url
        """
        response = self.__sqs_client.get_queue_url(QueueName=self.__config.input_queue)
        if not response or ("QueueUrl" not in response):
            raise InitializationError("Invalid get queue url reponse")
        return response["QueueUrl"]

    def get_message(self, queue_url: str) -> Optional[str]:
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

        if "Messages" in response:
            message = str(response["Messages"][0])
        return message

    def delete_message(self, input_queue_url: str, sqs_message: SQSMessage) -> None:
        """Deletes message from SQS queue

        Args:
            input_queue_url (str): the SQS queue url
            sqs_message (SQSMessage): the SQS queue to be deleted
        """
        _logger.info("deleting message -> %s", sqs_message)

        self.__sqs_client.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=sqs_message.receipt_handle
        )

    def __update_visibility_timeout(
            self,
            input_queue_url: str,
            sqs_message: SQSMessage,
            visibility_timeout_seconds: int) -> None:
        """Extends visibility timeout for artifacts that were not yet ingested

        Args:
            input_queue_url (str): SQS queue url
            sqs_message (SQSMessage): sqs_message
            visibility_timeout_seconds (int): new visibility timeout to be added in seconds
        """
        self.__sqs_client.change_message_visibility(
            QueueUrl=input_queue_url,
            ReceiptHandle=sqs_message.receipt_handle,
            VisibilityTimeout=visibility_timeout_seconds
        )

    def increase_visibility_timeout_and_handle_exceptions(self, queue_url: str, sqs_message: SQSMessage) -> None:
        """Increase the visibility timeout to the maximum of 12 hours and handles AWS SDK exceptions

        Args:
            queue_url (str): the SQS queue url
            sqs_message (SQSMessage): the SQS message
        """
        try:
            self.__update_visibility_timeout(queue_url, sqs_message, TWELVE_HOURS_IN_SECONDS)
        except (aws_errors.MessageNotInflight, aws_errors.ReceiptHandleIsInvalid) as error:
            _logger.error("error updating visbility timeout %s", error)
        except botocore.exceptions.ClientError as error:
            _logger.error("unexpected AWS SDK error %s", error)
