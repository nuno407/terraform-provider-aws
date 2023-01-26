# type: ignore
"""Ridecare healthcheck module."""
import logging
import time
from typing import Callable, Dict

import botocore.exceptions
from kink import inject

from base.graceful_exit import GracefulExit
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.checker.common import ArtifactChecker
from healthcheck.config import HealthcheckConfig
from healthcheck.constants import (ELASTIC_ALERT_MATCHER, TWELVE_HOURS_IN_SECONDS,
                                   ELASTIC_SUCCESS_MATCHER, LOOP_DELAY_SECONDS)
from healthcheck.controller.aws_sqs import SQSMessageController
from healthcheck.exceptions import (FailedHealthCheckError,
                                    InvalidMessageCanSkip, InvalidMessageError,
                                    InvalidMessagePanic, NotYetIngestedError)
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import Artifact, ArtifactType, SQSMessage, VideoArtifact
from healthcheck.notification import Notifier

logger: logging.Logger = logging.getLogger(__name__)


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
            sqs_controller: SQSMessageController,
            notifier: Notifier,
            checkers: Dict[ArtifactType, ArtifactChecker]):
        self.__config = config
        self.__graceful_exit = graceful_exit
        self.__sqs_msg_parser = sqs_msg_parser
        self.__artifact_msg_parser = artifact_msg_parser
        self.__sqs_controller = sqs_controller
        self.__notifier = notifier
        self.__checkers = checkers

    def is_blacklisted_tenant(self, message: SQSMessage) -> bool:
        """Check if message is from blacklisted tenant.

        Args:
            message (SQSMessage): parsed SQS message

        Returns:
            bool: verification result
        """
        return any(message.attributes.tenant == tenant for tenant in self.__config.tenant_blacklist)

    def is_blacklisted_recorder(self, message: Artifact) -> bool:
        """Check if message is from blacklisted recorder type

        Args:
            message (Artifact): artifact message with type inside id

        Returns:
            bool: verification result
        """
        return any(recorder in message.artifact_id for recorder in self.__config.recorder_blacklist)

    def is_whitelisted_training(self, artifact: Artifact) -> bool:
        """Check if message is a black listed training

        Args:
            message (SQSMessage): artifact message with type inside id

        Returns:
            bool: verification result
        """
        if isinstance(artifact, VideoArtifact):
            return artifact.tenant_id in self.__config.training_whitelist

        return False

    def alert(self, artifact: Artifact, message: str) -> None:
        """Emit log entry to trigger Kibana alert

        Args:
            message (str): message to be displayed
        """
        logger.info("%s : %s : [%s]", ELASTIC_ALERT_MATCHER,
                    message, artifact.artifact_id)
        self.__notifier.send_notification(
            f"{message} : [{artifact.artifact_id}]")

    def __check_artifacts(self, artifacts: list[Artifact], queue_url: str, sqs_message: SQSMessage):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            queue_url (str): input queue url
            sqs_message (SQSMessage): SQS message
        """
        for artifact in artifacts:
            if artifact.artifact_type == ArtifactType.TRAINING_RECORDER and \
                    not self.is_whitelisted_training(artifact):
                logger.info(
                    "Ignoring, artifact is not training recorder whitelisted")
                self.__sqs_controller.delete_message(
                    queue_url, sqs_message)
                return
            if self.is_blacklisted_recorder(artifact):
                logger.info("Ignoring blacklisted recorder: %s",
                            artifact.artifact_id)
                self.__sqs_controller.delete_message(queue_url, sqs_message)
                return
            self.__check_artifact(artifact, queue_url, sqs_message)

    def __check_artifact(self, artifact: Artifact, queue_url: str, sqs_message: SQSMessage):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            queue_url (str): input queue url
            sqs_message (SQSMessage): SQS message
        """
        logger.info("artifact : %s", artifact)
        try:
            self.__checkers[artifact.artifact_type].run_healthcheck(artifact)
            logger.info("%s : %s", ELASTIC_SUCCESS_MATCHER,
                        artifact.artifact_id)
            self.__sqs_controller.delete_message(queue_url, sqs_message)
        except NotYetIngestedError as err:
            logger.warning(
                "not ingested yet, increase visibility timeout %s", err)
            self.__sqs_controller.try_update_message_visibility_timeout(
                queue_url, sqs_message, TWELVE_HOURS_IN_SECONDS)
        except FailedHealthCheckError as err:
            self.alert(err.artifact, err.message)
        except botocore.exceptions.ClientError as error:
            logger.error("unexpected AWS SDK error %s", error)
        except Exception as err:
            logger.error("unexpected error %s", err)
            raise err

    def run(self, helper_continue_running: Callable[[], bool] = lambda: True) -> None:
        """
        Main loop

        Parses queue message filter blacklisted tenants and recorders and finally
        runs healthchecks
        """
        queue_url = self.__sqs_controller.get_queue_url()

        while self.__graceful_exit.continue_running and helper_continue_running():
            logger.debug("waiting %s seconds to pull next message",
                         LOOP_DELAY_SECONDS)
            time.sleep(LOOP_DELAY_SECONDS)

            raw_message = self.__sqs_controller.get_message(queue_url)
            if not raw_message:
                continue

            try:
                sqs_message = self.__sqs_msg_parser.parse_message(raw_message)
            except InvalidMessagePanic:
                logger.exception("invalid message coming from SQS")
                raise

            if self.is_blacklisted_tenant(sqs_message):
                logger.info("Ignoring blacklisted tenant message: %s",
                            sqs_message.attributes.tenant)
                self.__sqs_controller.delete_message(queue_url, sqs_message)
                continue

            try:
                artifacts = self.__artifact_msg_parser.parse_message(
                    sqs_message)

                self.__check_artifacts(artifacts, queue_url, sqs_message)
            except InvalidMessageError as err:
                logger.error("Error parsing artifact from message -> %s", err)
                continue
            except InvalidMessageCanSkip as err:
                logger.info(
                    "Exception parsing artifact from the message. Exception: %s", {err})
                self.__sqs_controller.delete_message(queue_url, sqs_message)
