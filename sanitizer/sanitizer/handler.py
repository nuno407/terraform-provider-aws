""" handler module. """
import logging
from typing import Callable, Optional

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import SQSController, SQSMessage
from base.graceful_exit import GracefulExit
from sanitizer.artifact.artifact_filter import ArtifactFilter
from sanitizer.artifact.artifact_forwarder import ArtifactForwarder
from sanitizer.artifact.artifact_parser import ArtifactParser
from sanitizer.exceptions import ArtifactException, MessageException
from sanitizer.message.message_filter import MessageFilter
from sanitizer.message.message_parser import MessageParser
from sanitizer.message.message_persistence import MessagePersistence

__logger = logging.getLogger(__name__)


@inject
class Handler:
    """ message handler """

    def __init__(self,
                 aws_sqs_controller: SQSController,
                 message_parser: MessageParser,
                 message_filter: MessageFilter,
                 message_persistence: MessagePersistence,
                 artifact_parser: ArtifactParser,
                 artifact_filter: ArtifactFilter,
                 artifact_forwarder: ArtifactForwarder) -> None:
        self.aws_sqs = aws_sqs_controller
        self.message_parser = message_parser
        self.message_filter = message_filter
        self.message_persistence = message_persistence
        self.artifact_parser = artifact_parser
        self.artifact_filter = artifact_filter
        self.forwarder = artifact_forwarder
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
                message: SQSMessage = self.message_parser.parse(raw_sqs_message)
                self._process_message(message)
            except MessageException as err:
                __logger.exception("SKIP: Unable to parse message -> %s", err)
                continue

    def _process_message(self, message: SQSMessage):
        """processes a single message to artifacts and publishes them"""
        is_relevant = self.message_filter.is_relevant(message)
        if not is_relevant:
            self.aws_sqs.delete_message(self.__queue_url, message)
            return

        self.message_persistence.save(message)

        artifacts = self.artifact_parser.parse(message)
        for artifact in artifacts:
            try:
                is_relevant = self.artifact_filter.is_relevant(artifact)
                if is_relevant:
                    self.forwarder.publish(artifact)
            except ArtifactException as err:
                __logger.exception("SKIP: Unable to parse artifact -> %s", err)
                continue
        self.aws_sqs.delete_message(self.__queue_url, message)
