"""Ridecare healthcheck module."""
import logging
from typing import Dict, Callable

import botocore.exceptions
from kink import inject

from base.graceful_exit import GracefulExit
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.checker.artifact import BaseArtifactChecker
from healthcheck.config import HealthcheckConfig
from healthcheck.constants import ELASTIC_ALERT_MATCHER
from healthcheck.controller.aws_sqs import SQSMessageController
from healthcheck.exceptions import (FailedHealthCheckError,
                                    InvalidMessageError, NotPresentError,
                                    NotYetIngestedError)
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import Artifact, ArtifactType, SQSMessage

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
            checkers: Dict[ArtifactType, BaseArtifactChecker]):
        self.__config = config
        self.__graceful_exit = graceful_exit
        self.__sqs_msg_parser = sqs_msg_parser
        self.__artifact_msg_parser = artifact_msg_parser
        self.__sqs_controller = sqs_controller
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
        logger.info("%s : %s", ELASTIC_ALERT_MATCHER, message)

    def __check_artifact(self, artifact: Artifact, queue_url: str, sqs_message: SQSMessage):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            queue_url (str): input queue url
            sqs_message (SQSMessage): SQS message
        """
        logger.info("artifact -> %s", artifact)
        try:
            self.__checkers[artifact.artifact_type].run_healthcheck(artifact)
            logger.info("healthcheck success -> %s", artifact.artifact_id)
            self.__sqs_controller.delete_message(queue_url, sqs_message)
        except InvalidMessageError as err:
            logger.error("invalid message -> %s", err)
        except NotYetIngestedError as err:
            logger.warning("not ingested yet, increase visibility timeout %s", err)
            self.__sqs_controller.increase_visibility_timeout_and_handle_exceptions(queue_url, sqs_message)
        except NotPresentError as err:
            self.alert(err.message)
        except FailedHealthCheckError as err:
            self.alert(err.message)
        except botocore.exceptions.ClientError as error:
            logger.error("unexpected AWS SDK error %s", error)

    def run(self, helper_continue_running: Callable[[], bool] = lambda: True) -> None:
        """
        Main loop

        Parses queue message filter blacklisted tenants and recorders and finally
        runs healthchecks
        """
        queue_url = self.__sqs_controller.get_queue_url()
        while self.__graceful_exit.continue_running and helper_continue_running():
            raw_message = self.__sqs_controller.get_message(queue_url)
            if not raw_message:
                continue

            sqs_message = self.__sqs_msg_parser.parse_message(raw_message)
            if self.is_blacklisted_tenant(sqs_message):
                logger.info("ignoring blacklisted tenant message")
                continue

            artifacts = self.__artifact_msg_parser.parse_message(sqs_message)
            for artifact in artifacts:
                if self.is_blacklisted_recorder(artifact):
                    logger.info("ignoring blacklisted recorder")
                    continue

                self.__check_artifact(artifact, queue_url, sqs_message)
