""" handler module. """
from typing import Callable, Optional

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import SQSController, SQSMessage
from base.graceful_exit import GracefulExit
from sanitizer.artifact.filter import ArtifactFilter
from sanitizer.artifact.forwarder import ArtifactForwarder
from sanitizer.artifact.parser import ArtifactParser
from sanitizer.message.filter import MessageFilter
from sanitizer.message.parser import MessageParser


@inject
class Handler:
    """ message handler """

    def __init__(self,
                 aws_sqs_controller: SQSController,
                 message_parser: MessageParser,
                 message_filter: MessageFilter,
                 artifact_filter: ArtifactFilter,
                 artifact_parser: ArtifactParser,
                 forwarder: ArtifactForwarder) -> None:
        self.aws_sqs = aws_sqs_controller
        self.message_parser = message_parser
        self.message_filter = message_filter
        self.artifact_parser = artifact_parser
        self.artifact_filter = artifact_filter
        self.forwarder = forwarder

    @inject
    def run(self, graceful_exit: GracefulExit,
            helper_continue_running: Callable[[], bool] = lambda: True):
        """handler incoming message and apply parsers and filters"""
        queue_url = self.aws_sqs.get_queue_url()

        while graceful_exit.continue_running and helper_continue_running():
            raw_sqs_message: Optional[MessageTypeDef] = self.aws_sqs.get_message(queue_url)
            if raw_sqs_message is None:
                continue

            message: SQSMessage = self.message_parser.parse(raw_sqs_message)

            is_relevant = self.message_filter.is_relevant(message)
            if not is_relevant:
                self.aws_sqs.delete_message(queue_url, message)
                continue

            artifacts = self.artifact_parser.parse(message)
            artifacts = self.artifact_filter.apply(artifacts)
            for artifact in artifacts:
                self.forwarder.publish(artifact)
            self.aws_sqs.delete_message(queue_url, message)
