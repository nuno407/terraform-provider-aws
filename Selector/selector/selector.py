""" Selector component bussiness logic. """
import logging
import os
from json import loads

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.s3 import S3Controller
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import PreviewSignalsArtifact, parse_artifact, Artifact, VideoArtifact, RecorderType
from selector.decision import Decision
from selector.evaluator import Evaluator
from selector.footage_api_wrapper import FootageApiWrapper
from selector.model import PreviewMetadata, parse

_logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", None)


@inject
class Selector:  # pylint: disable=too-few-public-methods
    """ Class responsible by containing all bussiness logic used in in the Selector component. """

    def __init__(self, s3_controller: S3Controller,
                 footage_api_wrapper: FootageApiWrapper,
                 sqs_controller: SQSController,
                 evaluator: Evaluator):
        self.__sqs_controller = sqs_controller
        self.footage_api_wrapper = footage_api_wrapper
        self.s3_controller = s3_controller
        self.evaluator = evaluator

    @inject
    def run(self, graceful_exit: GracefulExit) -> None:
        """ Function responsible for running the component (component entrypoint). """
        _logger.info("Starting Selector..")

        while graceful_exit.continue_running:
            message = self.__sqs_controller.get_message()
            if not message:
                continue
            self.__handle_message(message)

    def __handle_message(self, message: MessageTypeDef):
        """ Function responsible for processing messages from SQS (component entrypoint). """
        success = self.__message_dispatcher(message)
        if success:
            self.__sqs_controller.delete_message(message)

    def __message_dispatcher(self, message: MessageTypeDef) -> bool:
        """
        Will parse the artifact comming from the queue and redirect it to the correct handler.

        Args:
            message (MessageTypeDef): Message from the queue

        Returns:
            bool: True if everything was process correctly
        """

        # Build artifact
        recorder_sqs_message = message["Body"]
        artifact: Artifact = parse_artifact(recorder_sqs_message)

        if isinstance(artifact, PreviewSignalsArtifact):
            return self.__process_preview_interior(artifact)
        if isinstance(artifact, VideoArtifact) and artifact.recorder == RecorderType.INTERIOR:
            return self.__process_video_recorder(artifact)

        return False

    def __process_video_recorder(self, video_artifact: VideoArtifact) -> bool:
        """Logic to call the footage upload request for interior recorder

        Args:
            video_artifact (VideoArtifact): Video artifact grabbed from the queue

        Returns:
            bool: Boolean indicating if the request succeeded.
        """
        try:
            self.footage_api_wrapper.request_recorder(
                RecorderType.TRAINING,
                video_artifact.device_id,
                video_artifact.timestamp,
                video_artifact.end_timestamp)
        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting footage: %s", error)
            return False

        return True

    def __process_preview_interior(self, preview_metadata_artifact: PreviewSignalsArtifact) -> bool:
        """Logic to call the footage upload request

        Args:
            preview_metadata_artifact (PreviewSignalsArtifact): Preview artifact grabbed from the queue

        Returns:
            bool: Boolean indicating if the request succeeded.
        """

        # get the metadata from s3
        # location of concatenated preview metadata within the DevCloud S3
        bucket, key = self.s3_controller.get_s3_path_parts(preview_metadata_artifact.s3_path)
        preview_metadata_bytes = self.s3_controller.download_file(bucket, key)
        preview_metadata_json = loads(preview_metadata_bytes.decode())

        # build PreviewMetadata object and pass it to evaluation
        preview_metadata: PreviewMetadata = parse(preview_metadata_json)
        decisions: list[Decision] = self.evaluator.evaluate(
            preview_metadata, preview_metadata_artifact)

        # if there is something to be requested
        for decision in decisions:
            try:
                self.footage_api_wrapper.request_recorder(
                    decision.recorder,
                    preview_metadata_artifact.device_id,
                    decision.footage_from,
                    decision.footage_to)
            except Exception as error:  # pylint: disable=broad-except
                _logger.error("Unexpected error occured when requesting footage: %s", error)
                return False

        if len(decisions) == 0:
            _logger.info("Ride was not selected for training upload by any wule")
        return True
