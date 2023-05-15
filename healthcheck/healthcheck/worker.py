"""Ridecare healthcheck module."""
import json
import logging
from typing import Dict

import botocore.exceptions
from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import TWELVE_HOURS_IN_SECONDS, SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  VideoArtifact, parse_artifact)
from healthcheck.checker.common import ArtifactChecker  # type: ignore
from healthcheck.config import HealthcheckConfig
from healthcheck.constants import (ELASTIC_ALERT_MATCHER,
                                   ELASTIC_SUCCESS_MATCHER)
from healthcheck.exceptions import FailedHealthCheckError, NotYetIngestedError
from healthcheck.notification import Notifier

logger: logging.Logger = logging.getLogger(__name__)


@inject
class HealthCheckWorker:
    """
    This is specific to RCC.
    In the future we might have the need to handle a new cloud.
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            config: HealthcheckConfig,
            sqs_controller: SQSController,
            notifier: Notifier,
            checkers: Dict[RecorderType, ArtifactChecker]):
        self.__config = config
        self.__sqs_controller = sqs_controller
        self.__notifier = notifier
        self.__checkers = checkers

    def is_relevant(self, artifact: Artifact) -> bool:
        """ Check if artifact is relevant """
        if artifact.recorder == RecorderType.TRAINING:
            return self.__is_whitelisted_training(artifact)

        return True

    def __is_whitelisted_training(self, artifact: Artifact) -> bool:
        """Check if message is a black listed training

        Args:
            message (SQSMessage): artifact message with type inside id

        Returns:
            bool: verification result
        """
        return artifact.tenant_id in self.__config.training_whitelist

    def alert(self, artifact_id: str, message: str) -> None:
        """Emit log entry to trigger Kibana alert

        Args:
            message (str): message to be displayed
        """
        logger.info("%s : %s : [%s]", ELASTIC_ALERT_MATCHER, message, artifact_id)
        self.__notifier.send_notification(f"{message} : [{artifact_id}]")

    def __deserialize(self, raw_message: str) -> str:
        call_args = [("'", '"'), ("\\n", ""), ("\\\\", ""),  # pylint: disable=invalid-string-quote
                     ("\\", ""), ('"{', "{"), ('}"', "}")]   # pylint: disable=invalid-string-quote
        for args in call_args:
            raw_message = raw_message.replace(args[0], args[1])
        return raw_message

    @inject
    def run(self, graceful_exit: GracefulExit) -> None:
        """
        Main loop

        Parses queue message filter blacklisted tenants and recorders and finally
        runs healthchecks
        """
        while graceful_exit.continue_running:
            raw_message = self.__sqs_controller.get_message()
            if not raw_message:
                continue

            raw_body = self.__deserialize(raw_message["Body"])
            parsed_body = json.loads(raw_body)

            artifact = parse_artifact(parsed_body["Message"])
            if not isinstance(artifact, VideoArtifact) \
                    and not isinstance(artifact, SnapshotArtifact):
                logger.info("SKIP, artifact is not video or snapshot")
                self.__sqs_controller.delete_message(raw_message)
                continue

            if artifact.recorder not in self.__checkers.keys():
                logger.info("SKIP, artifact is not handled by healthcheck")
                self.__sqs_controller.delete_message(raw_message)
                continue

            if not self.is_relevant(artifact):
                logger.info("SKIP, artifact is not training recorder whitelisted")
                self.__sqs_controller.delete_message(raw_message)
                continue

            self.__check_artifact(artifact, raw_message)

    def __check_artifact(self, artifact: Artifact, raw_message: MessageTypeDef):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            sqs_message (SQSMessage): SQS message
        """
        logger.info("artifact : %s", artifact)
        try:
            self.__checkers[artifact.recorder].run_healthcheck(artifact)
            logger.info("%s : %s", ELASTIC_SUCCESS_MATCHER,
                        artifact.artifact_id)
            self.__sqs_controller.delete_message(raw_message)
        except NotYetIngestedError as err:
            logger.warning(
                "not ingested yet, increase visibility timeout %s", err)
            self.__sqs_controller.try_update_message_visibility_timeout(
                raw_message, TWELVE_HOURS_IN_SECONDS)
        except FailedHealthCheckError as err:
            self.alert(err.artifact_id, err.message)
        except botocore.exceptions.ClientError as error:
            logger.error("unexpected AWS SDK error %s", error)
        except Exception as err:
            logger.error("unexpected error %s", err)
            raise err
