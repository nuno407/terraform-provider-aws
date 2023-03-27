""" handler module. """
from typing import Callable

from kink import inject

from sanitizer.artifact.filter import ArtifactFilter
from sanitizer.artifact.forwarder import ArtifactForwarder
from sanitizer.artifact.parser import ArtifactParser
from sanitizer.aws.sqs import AWSSQSController
from sanitizer.message.filter import MessageFilter
from sanitizer.message.parser import MessageParser


@inject
class Handler:
    """ message handler """

    def __init__(self,
                 aws_sqs_controller: AWSSQSController,
                 message_parser: MessageParser,
                 message_filter: MessageFilter,
                 artifact_filter: ArtifactFilter,
                 artifact_parser: ArtifactParser,
                 forwarder: ArtifactForwarder) -> None:
        self.aws_sqs_controller = aws_sqs_controller
        self.message_parser = message_parser
        self.message_filter = message_filter
        self.artifact_parser = artifact_parser
        self.artifact_filter = artifact_filter
        self.forwarder = forwarder


    def run(self, helper_continue_running: Callable[[], bool] = lambda: True):
        """handler incoming message and apply parsers and filters"""
        queue_url = self.aws_sqs_controller.get_queue_url()
        while helper_continue_running():
            raw_sqs_message = self.aws_sqs_controller.get_message(queue_url)
            if not raw_sqs_message:
                continue

            message = self.message_parser.parse(raw_sqs_message)
            message = self.message_filter.apply(message)
            if not message:
                continue

            artifacts = self.artifact_parser.parse(message)
            artifacts = self.artifact_filter.apply(artifacts)
            for artifact in artifacts:
                self.forwarder.publish(artifact)
            self.aws_sqs_controller.delete_message(queue_url, message)
