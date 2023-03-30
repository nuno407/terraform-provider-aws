# type: ignore
""" handler module. """
import logging
from typing import Callable, Optional

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import SQSController, SQSMessage
from base.graceful_exit import GracefulExit
from sanitizer.artifact.artifact_controller import ArtifactController
from sanitizer.exceptions import ArtifactException, MessageException
from sanitizer.message.message_controller import MessageController

__logger = logging.getLogger(__name__)


@inject
class Handler:  # pylint: disable=too-few-public-methods
    """ message handler """

    def __init__(self,
                 aws_sqs_controller: SQSController,
                 message: MessageController,
                 artifact: ArtifactController) -> None:
        self.aws_sqs = aws_sqs_controller
        self.message = message
        self.artifact = artifact
        self.__queue_url = self.aws_sqs.get_queue_url()

    @inject
    def run(self, graceful_exit: GracefulExit,
            helper_continue_running: Callable[[], bool] = lambda: True):
        """retrieves incoming messages, parses them and forwards them to the next step"""
        while graceful_exit.continue_running and helper_continue_running():
            raw_sqs_message: Optional[MessageTypeDef] = self.aws_sqs.get_message(self.__queue_url)
            if raw_sqs_message is None:
                continue

            try:
                message: SQSMessage = self.message.parser.parse(raw_sqs_message)
                self._process_message(message)
            except MessageException as err:
                __logger.exception("SKIP: Unable to parse message -> %s", err)
                continue

    def _process_message(self, message: SQSMessage):
        """processes a single message to artifacts and publishes them"""
        is_relevant = self.message.filter.is_relevant(message)
        if not is_relevant:
            self.aws_sqs.delete_message(self.__queue_url, message)
            return

        self.message.persistence.save(message)

        artifacts = self.artifact.parser.parse(message)
        for artifact in artifacts:
            try:
                is_relevant = self.artifact.filter.is_relevant(artifact)
                if is_relevant:
                    self.artifact.forwarder.publish(artifact)
            except ArtifactException as err:
                __logger.exception("SKIP: Unable to parse artifact -> %s", err)
                continue
        self.aws_sqs.delete_message(self.__queue_url, message)
