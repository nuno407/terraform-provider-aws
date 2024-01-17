# type: ignore
""" handler module. """
import logging
from typing import Optional
from dataclasses import dataclass

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.model import SQSMessage
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import Artifact, ImageBasedArtifact, EventArtifact, OperatorArtifact, DeviceInfoEventArtifact
from sanitizer.artifact.artifact_controller import ArtifactController
from sanitizer.exceptions import ArtifactException, MessageException
from sanitizer.message.message_controller import MessageController
from sanitizer.device_info_db_client import DeviceInfoDBClient

_logger = logging.getLogger(__name__)


@dataclass
class ArtifactDispatch:
    """Dataclass representing messages that should be published to metadata SQS or SNS"""
    sns_artifacts: list[Artifact]
    sqs_artifacts: list[Artifact]

    def merge(self, other: "ArtifactDispatch"):
        """
        Merges the artifacts of another ArtifactDispatch into this one.

        Args:
            other (ArtifactDispatch): The other ArtifactDispatch to merge
        """
        self.sns_artifacts.extend(other.sns_artifacts)
        self.sqs_artifacts.extend(other.sqs_artifacts)


@inject
class Handler:  # pylint: disable=too-few-public-methods
    """ message handler """

    def __init__(self,  # pylint: disable=too-many-arguments
                 metadata_sqs_controller: SQSController,
                 aws_sqs_rcc_controller: SQSController,
                 message: MessageController,
                 artifact: ArtifactController,
                 device_info_db: DeviceInfoDBClient) -> None:
        self.__metadata_sqs_controller = metadata_sqs_controller
        self.__aws_sqs_rcc_controller = aws_sqs_rcc_controller
        self.__message = message
        self.__artifact = artifact
        self.__device_info_db = device_info_db

    @inject
    def run(self, graceful_exit: GracefulExit):
        """retrieves incoming messages, parses them and forwards them to the next step"""
        while graceful_exit.continue_running:
            raw_sqs_message: Optional[MessageTypeDef] = self.__aws_sqs_rcc_controller.get_message()
            _logger.debug("receveid raw message %s", raw_sqs_message)
            if raw_sqs_message is None:
                continue

            try:
                message: SQSMessage = self.__message.parser.parse(raw_sqs_message)
                _logger.debug("parsed message %s", message)
                self.__process_message(message)
            except MessageException as err:
                _logger.exception("SKIP: Unable to parse message -> %s, message will not be deleted", err)
                continue

    def __get_injected_artifacts(self, origin_artifact: ImageBasedArtifact) -> ArtifactDispatch:
        """
        Injects artifacts into the pipeline and returns the artifacts to be dispatched.

        Args:
            artifact (ImageBasedArtifact): The artifact to inject

        Returns:
            ArtifactDispatch:  The artifacts to dispatch
        """

        artifacts = self.__artifact.injector.inject(origin_artifact)
        result = ArtifactDispatch([], [])

        for artifact in artifacts:
            result.merge(self.__handle_artifact(artifact))

        return result

    def __dispatch_artifacts(self, artifact_dispatch: ArtifactDispatch):
        """
        Dispatches artifacts to the next step in the pipeline.

        Args:
            artifact_dispatch (ArtifactDispatch): The artifacts to dispatch
        """
        for artifact in artifact_dispatch.sns_artifacts:
            self.__artifact.forwarder.publish(artifact)

        for artifact in artifact_dispatch.sqs_artifacts:
            self.__metadata_sqs_controller.send_message(artifact.stringify())

    def __handle_artifact(self, artifact: Artifact) -> ArtifactDispatch:
        """
        Handles an artifact by injecting additional artifacts if relevant and
        returning all artifacts to be disptached to the respective step.

        This ensures that all artifacts are processed and parsed before being sent

        Args:
            artifact (Artifact): The artifact to handle

        Returns:
            ArtifactDispatch: The artifacts to dispatch
        """

        result = ArtifactDispatch([], [])

        if isinstance(artifact, ImageBasedArtifact):
            # Recursively inject artifacts
            result.merge(self.__get_injected_artifacts(artifact))

        if not self.__artifact.filter.is_relevant(artifact):
            _logger.info(
                "SKIP: artifact is irrelevant - tenant=%s device_id=%s",
                artifact.tenant_id,
                artifact.device_id)
            return result

        if isinstance(artifact, EventArtifact):
            result.sqs_artifacts.append(artifact)
            if isinstance(artifact, DeviceInfoEventArtifact):
                self.__device_info_db.store_device_information(artifact)
            return result

        if isinstance(artifact, OperatorArtifact):
            result.sqs_artifacts.append(artifact)

        result.sns_artifacts.append(artifact)
        return result

    def __process_message(self, message: SQSMessage):
        """
        Processes a single message to artifacts and publishes them.
        """

        if not self.__message.filter.is_relevant(message):
            _logger.info("SKIP: message is irrelevant - message_id=%s: tenant=%s",
                         message.message_id,
                         message.attributes.tenant)
            self.__aws_sqs_rcc_controller.delete_message(message)
            return

        self.__message.persistence.save(message)
        try:
            artifacts = self.__artifact.parser.parse(message)
            artifact_dispatch = ArtifactDispatch([], [])
            for artifact in artifacts:
                artifact_dispatch.merge(self.__handle_artifact(artifact))

            self.__dispatch_artifacts(artifact_dispatch)
            self.__aws_sqs_rcc_controller.delete_message(message)
        except ArtifactException as err:
            _logger.error("SKIP: Unable to parse artifacts -> %s, message will not be deleted", err)
