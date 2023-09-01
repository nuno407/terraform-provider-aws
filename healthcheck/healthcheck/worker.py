"""Ridecare healthcheck module."""
import logging

import botocore.exceptions
from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import (TWELVE_HOURS_IN_SECONDS, SQSController,
                          parse_message_body_to_dict)
from base.graceful_exit import GracefulExit
from base.model.artifacts import Artifact, parse_artifact
from healthcheck.checker.checker_determiner import CheckerDeterminer
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
            sqs_controller: SQSController,
            notifier: Notifier,
            checker_determiner: CheckerDeterminer):
        self.__sqs_controller = sqs_controller
        self.__notifier = notifier
        self.__checker_determiner = checker_determiner

    def alert(self, artifact_id: str, message: str) -> None:
        """Emit log entry to trigger Kibana alert

        Args:
            message (str): message to be displayed
        """
        logger.info("%s : %s : [%s]", ELASTIC_ALERT_MATCHER, message, artifact_id)
        self.__notifier.send_notification(f"{message} : [{artifact_id}]")

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

            parsed_body = parse_message_body_to_dict(raw_message["Body"])

            artifact = parse_artifact(parsed_body["Message"])
            self.__check_artifact(artifact, raw_message)

            self.__sqs_controller.delete_message(raw_message)
            continue

    def __check_artifact(self, artifact: Artifact, raw_message: MessageTypeDef):
        """Run healthcheck for given artifact and treats errors

        Args:
            artifact (Artifact): artifact to be checked
            sqs_message (SQSMessage): SQS message
        """
        logger.info("artifact : %s", artifact)
        try:
            checker = self.__checker_determiner.get_checker(artifact)
            checker.run_healthcheck(artifact)
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
