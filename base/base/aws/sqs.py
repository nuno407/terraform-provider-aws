"""AWS SQS controller."""
import logging
from datetime import datetime
from typing import Optional, Union

import botocore.exceptions
from aws_error_utils import errors as aws_errors
from expiringdict import ExpiringDict  # type: ignore
from kink import inject
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.model import SQSMessage

TWELVE_HOURS_IN_SECONDS = 60 * 60 * 12
MAX_MESSAGE_VISIBILITY_TIMEOUT = 43200
MESSAGE_VISIBILITY_TIMEOUT_BUFFER = 0.5

_logger: logging.Logger = logging.getLogger(__name__)


class InitializationError(Exception):
    """Error raised during initialization."""


@inject
class SQSController:
    """SQS message controller."""

    def __init__(self,
                 default_sqs_queue_name: str,
                 sqs_client: SQSClient):
        self.input_queue_name = default_sqs_queue_name
        self.__sqs_client = sqs_client
        self.__message_receive_times: ExpiringDict[str, datetime] = ExpiringDict(
            max_len=1000, max_age_seconds=50400)
        self.__queue_url = self.__get_queue_url()

    def __get_queue_url(self, queue_name: Optional[str] = None) -> str:
        """Get queue url.

        This should happen only once during initialization of the service.

        Return:
            str: queue url
        """
        if not queue_name:
            queue_name = self.input_queue_name
        response = self.__sqs_client.get_queue_url(
            QueueName=queue_name)
        if not response or ("QueueUrl" not in response):
            raise InitializationError("Invalid get queue url reponse")
        return response["QueueUrl"]

    def get_message(self, wait_time : int = 20) -> Optional[MessageTypeDef]:
        """Get SQS queue message

        Returns:
            Optional[str]: raw message if any in response or None
        """
        message = None
        response = self.__sqs_client.receive_message(
            QueueUrl=self.__queue_url,
            AttributeNames=[
                "SentTimestamp",
                "ApproximateReceiveCount"
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                "All"
            ],
            WaitTimeSeconds=wait_time
        )

        if "Messages" in response and len(response["Messages"]) > 0:
            message = response["Messages"][0]
            self.__message_receive_times[message["ReceiptHandle"]] = datetime.now(
            )
        _logger.info("receiving message -> %s", message)
        return message

    def __delete_message(self, receipt_handle: str) -> None:
        """Deletes a message from SQS

        Args:
            receipt_handle (str): receipt handl of the message to be deleted
        """
        self.__message_receive_times.pop(receipt_handle, None)
        self.__sqs_client.delete_message(
            QueueUrl=self.__queue_url,
            ReceiptHandle=receipt_handle
        )

    def delete_message(self, sqs_message: Union[MessageTypeDef, SQSMessage]) -> None:
        """Deletes message from SQS queue

        Args:
            sqs_message (MessageTypeDef): the SQS message to be deleted
        """
        _logger.info("deleting message -> %s", sqs_message)
        if isinstance(sqs_message, SQSMessage):
            receipt_handle = sqs_message.receipt_handle
        else:
            receipt_handle = sqs_message["ReceiptHandle"]
        self.__delete_message(receipt_handle)

    def __update_visibility_timeout(
            self,
            receipt_handle: str,
            visibility_timeout_seconds: int) -> None:
        """Extends visibility timeout for artifacts that were not yet ingested

        Args:
            sqs_message (MessageTypeDef): Message as received by get_message
            visibility_timeout_seconds (int): new visibility timeout to be added in seconds
        """
        # Limit message visibility timeout to 12h
        get_received_timestamp: datetime = self.__message_receive_times.\
            get(receipt_handle, datetime.now())
        processing_time = int((datetime.now() - get_received_timestamp).total_seconds()
                              + 1.0 + MESSAGE_VISIBILITY_TIMEOUT_BUFFER)
        if visibility_timeout_seconds > MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time:
            visibility_timeout_seconds = MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time
            _logger.debug(
                "Limiting message visibility extension to %d seconds", visibility_timeout_seconds)

        self.__sqs_client.change_message_visibility(
            QueueUrl=self.__queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout_seconds
        )

    def try_update_message_visibility_timeout(self,
                                              sqs_message: Union[MessageTypeDef, SQSMessage],
                                              visibility_timeout_seconds: int) -> None:
        """Increase the visibility timeout to the maximum of 12 hours and handles AWS SDK exceptions

        Args:
            sqs_message (SQSMessage): the SQS message
        """
        if isinstance(sqs_message, SQSMessage):
            receipt_handle = sqs_message.receipt_handle
        else:
            receipt_handle = sqs_message["ReceiptHandle"]
        try:
            self.__update_visibility_timeout(
                receipt_handle, visibility_timeout_seconds)
        except (aws_errors.MessageNotInflight, aws_errors.ReceiptHandleIsInvalid) as error:
            _logger.error("error updating visbility timeout %s", error)
        except botocore.exceptions.ClientError as error:
            _logger.error("unexpected AWS SDK error %s", error)

    def send_message(self,
                     message: str,
                     source_container: str,
                     queue_name: Optional[str] = None) -> None:
        """Send message to SQS queue

        Args:
            message (str): message to be sent
            queue_name (str): name of the queue to send the message to,
            otherwise this uses the default queue of the client
        """
        if queue_name:
            queue_url = self.__get_queue_url(queue_name)
        else:
            queue_url = self.__queue_url

        self.__sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message,
            MessageAttributes={
                "SourceContainer": {
                    "StringValue": source_container,
                    "DataType": "String"
                }
            }
        )
