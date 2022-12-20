"""Ridecare healthcheck module."""
import logging
from typing import Dict, Optional

import botocore.exceptions
from aws_error_utils import errors as aws_errors
from kink import inject
from mypy_boto3_sqs import SQSClient

from base.graceful_exit import GracefulExit
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.checker.artifact import BaseArtifactChecker
from healthcheck.config import HealthcheckConfig
from healthcheck.exceptions import (AnonymizedFileNotPresent,
                                    FailDocumentValidation,
                                    InitializationError, InvalidMessageError,
                                    NotPresentError, NotYetIngestedError,
                                    RawFileNotPresent, VoxelEntryNotPresent,
                                    VoxelEntryNotUnique)
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import Artifact, ArtifactType, SQSMessage
from healthcheck.constants import ELASTIC_ALERT_MATCHER, TWELVE_HOURS_IN_SECONDS

_logger: logging.Logger = logging.getLogger(__name__)


@inject
class HealthCheckWorker:
    """
    This is specific to RCC.
    In the future we might have the need to handle a new cloud.
    """

    def __init__(
            self,
            config: HealthcheckConfig,
            graceful_exit: GracefulExit,
            sqs_msg_parser: SQSMessageParser,
            artifact_msg_parser: ArtifactParser,
            sqs_client: SQSClient,
            checkers: Dict[ArtifactType, BaseArtifactChecker]):
        self.__config = config
        self.__sqs_client = sqs_client
        self.__graceful_exit = graceful_exit
        self.__sqs_msg_parser = sqs_msg_parser
        self.__artifact_msg_parser = artifact_msg_parser
        self.__checkers = checkers

    def is_blacklisted_tenant(self, message: SQSMessage) -> bool:
        """Check if message is from blacklisted tenant.

        Args:
            message (SQSMessage): parsed SQS message

        Returns:
            bool: verification result
        """
        return any([message.attributes.tenant == tenant for tenant in self.__config.tenant_blacklist])

    def is_blacklisted_recorder(self, message: Artifact) -> bool:
        """Check if message is from blacklisted recorder type

        Args:
            message (Artifact): artifact message with type inside id

        Returns:
            bool: verification result
        """
        return any([recorder in message.artifact_id for recorder in self.__config.recorder_blacklist])

    def alert(self, message: str) -> None:
        """Emit log entry to trigger Kibana alert

        Args:
            message (str): message to be displayed
        """
        _logger.info("%s : %s", ELASTIC_ALERT_MATCHER, message)

    def __get_queue_url(self) -> str:
        """Get queue url.

        This should happen only once during initialization of the service.

        Return:
            str: queue url
        """
        response = self.__sqs_client.get_queue_url(QueueName=self.__config.input_queue)
        if not response or ("QueueUrl" not in response):
            raise InitializationError("Invalid get queue url reponse")
        return response["QueueUrl"]

    def __get_message(self, queue_url: str) -> Optional[str]:
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

    def __delete_message(self, input_queue_url: str, sqs_message: SQSMessage) -> None:
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

    def __increase_visibility_timeout_and_handle_exceptions(self, queue_url: str, sqs_message: SQSMessage) -> None:
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

    def __check_artifact(self, artifact: Artifact, queue_url: str, sqs_message: SQSMessage):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            queue_url (str): input queue url
            sqs_message (SQSMessage): SQS message
        """
        _logger.info("artifact -> %s", artifact)
        try:
            self.__checkers[artifact.artifact_type].run_healthcheck(artifact)
            _logger.info("healthcheck success -> %s", artifact.artifact_id)
            self.__delete_message(queue_url, sqs_message)
        except InvalidMessageError as err:
            _logger.error("invalid message -> %s", err)
        except NotYetIngestedError as err:
            _logger.warning("not ingested yet, increase visibility timeout %s", err)
            self.__increase_visibility_timeout_and_handle_exceptions(queue_url, sqs_message)
        except (FailDocumentValidation, NotPresentError,
                RawFileNotPresent, VoxelEntryNotPresent, VoxelEntryNotUnique,
                AnonymizedFileNotPresent) as err:
            self.alert(err.message)
        except botocore.exceptions.ClientError as error:
            _logger.error("unexpected AWS SDK error %s", error)

    def run(self) -> None:
        """
        Main loop

        Parses queue message filter blacklisted tenants and recorders and finally
        runs healthchecks
        """
        queue_url = self.__get_queue_url()
        while self.__graceful_exit.continue_running:
            raw_message = self.__get_message(queue_url)
            if not raw_message:
                continue

            sqs_message = self.__sqs_msg_parser.parse_message(raw_message)
            if self.is_blacklisted_tenant(sqs_message):
                _logger.info("ignoring blacklisted tenant message")
                continue

            artifacts = self.__artifact_msg_parser.parse_message(sqs_message)
            for artifact in artifacts:
                if self.is_blacklisted_recorder(artifact):
                    _logger.info("ignoring blacklisted recorder")
                    continue

                self.__check_artifact(artifact, queue_url, sqs_message)
