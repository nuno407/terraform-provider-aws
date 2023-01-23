"""AWS SQS controller."""
import logging
from datetime import datetime
from typing import Optional

import botocore.exceptions
from aws_error_utils import errors as aws_errors
from expiringdict import ExpiringDict
from kink import inject
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

from healthcheck.config import HealthcheckConfig
from healthcheck.constants import TWELVE_HOURS_IN_SECONDS
from healthcheck.exceptions import InitializationError
from healthcheck.model import SQSMessage

MAX_MESSAGE_VISIBILITY_TIMEOUT = 43200
MESSAGE_VISIBILITY_TIMEOUT_BUFFER = 0.5

_logger: logging.Logger = logging.getLogger(__name__)


@inject
class SQSMessageController():
    """SQS message controller."""

    def __init__(self,
                 config: HealthcheckConfig,
                 sqs_client: SQSClient):
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

        # Limit message visibility timeout to 12h
        get_received_timestamp: datetime = self.__message_receive_times.\
            get(sqs_message.receipt_handle, datetime.now())
        processing_time = int((datetime.now() - get_received_timestamp).total_seconds()
                              + 1.0 + MESSAGE_VISIBILITY_TIMEOUT_BUFFER)
        if visibility_timeout_seconds > MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time:
            visibility_timeout_seconds = MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time
            _logger.debug(
                "Limiting message visibility extension to %d seconds", visibility_timeout_seconds)

        self.__sqs_client.change_message_visibility(
            QueueUrl=input_queue_url,
            ReceiptHandle=sqs_message.receipt_handle,
            VisibilityTimeout=visibility_timeout_seconds
        )

    def increase_visibility_timeout_and_handle_exceptions(self, queue_url: str, sqs_message: SQSMessage) -> None:  # pylint: disable=line-too-long
        """Increase the visibility timeout to the maximum of 12 hours and handles AWS SDK exceptions

        Args:
            queue_url (str): the SQS queue url
            sqs_message (SQSMessage): the SQS message
        """
        try:
            self.__update_visibility_timeout(
                queue_url, sqs_message, TWELVE_HOURS_IN_SECONDS)
        except (aws_errors.MessageNotInflight, aws_errors.ReceiptHandleIsInvalid) as error:
            _logger.error("error updating visbility timeout %s", error)
        except botocore.exceptions.ClientError as error:
            _logger.error("unexpected AWS SDK error %s", error)
