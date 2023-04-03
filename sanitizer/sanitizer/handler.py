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

_logger = logging.getLogger(__name__)


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

    @inject
    def run(self, graceful_exit: GracefulExit,
            helper_continue_running: Callable[[], bool] = lambda: True):
        """retrieves incoming messages, parses them and forwards them to the next step"""
        queue_url = self.aws_sqs.get_queue_url()
        _logger.info("SQS queue url: %s", queue_url)

        while graceful_exit.continue_running and helper_continue_running():
            raw_sqs_message: Optional[MessageTypeDef] = self.aws_sqs.get_message(queue_url)
            _logger.debug("receveid raw message %s", raw_sqs_message)
            if raw_sqs_message is None:
                continue

            try:
                message: SQSMessage = self.message.parser.parse(raw_sqs_message)
                _logger.debug("parsed message %s", message)
                self._process_message(message, queue_url)
            except MessageException as err:
                _logger.exception("SKIP: Unable to parse message -> %s", err)
                continue

    def _process_message(self, message: SQSMessage, queue_url: str):
        """processes a single message to artifacts and publishes them"""
        is_relevant = self.message.filter.is_relevant(message)
        if not is_relevant:
            _logger.info("SKIP: message is irrelevant - message_id=%s: tenant=%s",
                         message.message_id,
                         message.attributes.tenant)
            self.aws_sqs.delete_message(queue_url, message)
            return

        self.message.persistence.save(message)
        try:
            artifacts = self.artifact.parser.parse(message)
            for artifact in artifacts:
                _logger.info("checking artifact recorder=%s device_id=%s tenant_id=%s",
                             artifact.recorder.value,
                             artifact.device_id,
                             artifact.tenant_id)

                is_relevant = self.artifact.filter.is_relevant(artifact)
                if is_relevant:
                    self.artifact.forwarder.publish(artifact)
                else:
                    _logger.info("SKIP: artifact is irrelevant - tenant=%s device_id=%s",
                                 artifact.device_id,
                                 artifact.tenant_id)
        except ArtifactException as err:
            _logger.warning("SKIP: Unable to parse artifacts -> %s", err)
        self.aws_sqs.delete_message(queue_url, message)
