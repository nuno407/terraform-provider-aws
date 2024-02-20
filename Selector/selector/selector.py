""" Selector component bussiness logic. """
import logging
import os
from datetime import timedelta
from json import loads

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.s3 import S3Controller
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import (Artifact, OperatorArtifact, PreviewSignalsArtifact, RecorderType,
                                  S3VideoArtifact, SnapshotArtifact, VideoArtifact, parse_artifact,
                                  RuleOrigin, DEFAULT_RULE_NAME, DEFAULT_RULE_VERSION)
from selector.config import SelectorConfig
from selector.correlator import Correlator
from selector.decision import Decision
from selector.evaluator import Evaluator
from selector.footage_api_wrapper import FootageApiWrapper
from selector.model import PreviewMetadata, parse, DBDecision

_logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", None)


@inject
class Selector:  # pylint: disable=too-few-public-methods,too-many-locals
    """ Class responsible by containing all bussiness logic used in in the Selector component. """

    def __init__(self, s3_controller: S3Controller,  # pylint: disable=too-many-arguments
                 footage_api_wrapper: FootageApiWrapper,
                 sqs_controller: SQSController,
                 config: SelectorConfig,
                 evaluator: Evaluator,
                 correlator: Correlator):
        self.__sqs_controller = sqs_controller
        self.footage_api_wrapper = footage_api_wrapper
        self.s3_controller = s3_controller
        self.evaluator = evaluator
        self.correlator = correlator
        self.config = config

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
        Will parse the artifact coming from the queue and redirect it to the correct handler.

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
        if isinstance(artifact, OperatorArtifact):
            return self.__process_sav_operator(artifact)
        if isinstance(artifact, S3VideoArtifact) and artifact.recorder == RecorderType.TRAINING:
            return self.correlator.correlate_video_rules(artifact)
        if isinstance(artifact, SnapshotArtifact):
            return self.correlator.correlate_snapshot_rules(artifact)
        return False

    def __process_video_recorder(self, video_artifact: VideoArtifact) -> bool:
        """Logic to call the footage upload request for interior recorder

        Args:
            video_artifact (VideoArtifact): Video artifact grabbed from the queue

        Returns:
            bool: Boolean indicating if the request succeeded.
        """
        try:
            footage_id = self.footage_api_wrapper.request_recorder(
                RecorderType.TRAINING,
                video_artifact.device_id,
                video_artifact.timestamp,
                video_artifact.end_timestamp)

            DBDecision(rule_name=DEFAULT_RULE_NAME,
                       rule_version=DEFAULT_RULE_VERSION,
                       origin=RuleOrigin.INTERIOR,
                       tenant=video_artifact.tenant_id,
                       footage_id=footage_id,
                       footage_from=video_artifact.timestamp,
                       footage_to=video_artifact.end_timestamp).save_db_decision()

        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting SRX footage: %s", error)
            return False

        return True

    def __process_sav_operator(self, sav_operator_artifact: OperatorArtifact) -> bool:
        """Logic to request footage upload whenever an SAV Operator spectates the video stream of an incident

        Args:
            sav_operator_artifact (OperatorArtifact): Operator artifact, indicates device and time window

        Returns:
            bool: Boolean indicating if the request succeeded.
        """
        try:
            time_windows = [
                (sav_operator_artifact.event_timestamp - timedelta(seconds=self.config.upload_window_seconds_start),
                 sav_operator_artifact.event_timestamp + timedelta(seconds=self.config.upload_window_seconds_end)),
                (sav_operator_artifact.operator_monitoring_start,
                 sav_operator_artifact.operator_monitoring_end)]

            for start_time, end_time in time_windows:
                footage_id = self.footage_api_wrapper.request_recorder(
                    RecorderType.TRAINING,
                    sav_operator_artifact.device_id,
                    start_time,
                    end_time)

                DBDecision(rule_name=DEFAULT_RULE_NAME,
                           rule_version=DEFAULT_RULE_VERSION,
                           origin=RuleOrigin.SAV,
                           tenant=sav_operator_artifact.tenant_id,
                           footage_id=footage_id,
                           footage_from=start_time,
                           footage_to=end_time).save_db_decision()

                self.footage_api_wrapper.request_recorder(
                    RecorderType.SNAPSHOT,
                    sav_operator_artifact.device_id,
                    start_time, end_time)

        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting SAV footage: %s", error)
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

        # THIS SHOULD BE ONLY TEMPORARY UNTIL WE HAVE A BETTER WAY TO FILTER
        # In this case, we are enabling temporary logic to ingest Gridwise data
        if preview_metadata_artifact.tenant_id == "ridecare_companion_gridwise":
            _logger.info("Ingesting data from Gridwise tenant...")
        #    _logger.info("Skipping tenant_id %s", preview_metadata_artifact.tenant_id)
        #    return True

        # Identify recorders to request. This prevents duplicate requests from not being registered as different rules
        rule_dict: dict = {}
        for dec in decisions:
            rule_dict.setdefault(
                (dec.footage_from, dec.footage_to, dec.recorder), []).append(
                (dec.rule_name, dec.rule_version))

        # If there are any requests to be made, make them
        # In theory, should be just TRAINING recorders, but in case there are others, this will still work
        for footage_from, footage_to, recorder in rule_dict:
            try:
                footage_id: str = self.footage_api_wrapper.request_recorder(
                    recorder,
                    preview_metadata_artifact.device_id,
                    footage_from,
                    footage_to)

                for rule_name, rule_version in rule_dict[(footage_from, footage_to, recorder)]:
                    DBDecision(rule_name=rule_name,
                               rule_version=rule_version,
                               origin=RuleOrigin.PREVIEW,
                               tenant=preview_metadata_artifact.tenant_id,
                               footage_id=footage_id,
                               footage_from=footage_from,
                               footage_to=footage_to).save_db_decision()

            except Exception as error:  # pylint: disable=broad-except
                _logger.error("Unexpected error occured when requesting SRX footage from rule: %s", error)
                return False

        if len(rule_dict) <= 0 or len(decisions) <= 0:
            _logger.info("No Rule has been triggered, no upload request will be made.")
        return True
