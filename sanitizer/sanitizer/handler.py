# type: ignore
""" handler module. """
import logging
from typing import Optional

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.model import SQSMessage
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import Artifact
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
    def run(self, graceful_exit: GracefulExit):
        """retrieves incoming messages, parses them and forwards them to the next step"""
        while graceful_exit.continue_running:
            raw_sqs_message: Optional[MessageTypeDef] = self.aws_sqs.get_message()
            _logger.debug("receveid raw message %s", raw_sqs_message)
            if raw_sqs_message is None:
                continue

            try:
                message: SQSMessage = self.message.parser.parse(raw_sqs_message)
                _logger.debug("parsed message %s", message)
                self._process_message(message)
            except MessageException as err:
                _logger.exception("SKIP: Unable to parse message -> %s", err)
                continue

    def _inject_and_publish_additional_artifacts(self, artifact: Artifact):
        """injects and publishes additional artifacts if they are relevant
        INTERIOR_PREVIEW artifacts should inject preview signals artifacts,
        TRAINING and INTERIOR video artifacts should inject metadata artifacts
        before publishing them, in the future we might want to parse RCC messages
        instead of injecting those artifacts"""
        injected_artifacts = self.artifact.injector.inject(artifact)
        for injected in injected_artifacts:
            if self.artifact.filter.is_relevant(injected):
                self.artifact.forwarder.publish(injected)

    def _process_message(self, message: SQSMessage):
        """processes a single message to artifacts and publishes them"""
        is_relevant = self.message.filter.is_relevant(message)
        if not is_relevant:
            _logger.info("SKIP: message is irrelevant - message_id=%s: tenant=%s",
                         message.message_id,
                         message.attributes.tenant)
            self.aws_sqs.delete_message(message)
            return

        self.message.persistence.save(message)
        try:
            artifacts = self.artifact.parser.parse(message)
            for artifact in artifacts:
                try:
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
                    self._inject_and_publish_additional_artifacts(artifact)
                except ArtifactException as err:
                    _logger.warning("SKIP: Unable to publish artifact -> %s", err)
        except ArtifactException as err:
            _logger.warning("SKIP: Unable to parse artifacts -> %s", err)
        self.aws_sqs.delete_message(message)
