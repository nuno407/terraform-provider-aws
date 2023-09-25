""" Artifact Parser module. """
import logging

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import (Artifact, EventArtifact, MultiSnapshotArtifact,
                                  S3VideoArtifact, OperatorArtifact)
from sanitizer.artifact.artifact_type_parser import ArtifactTypeParser
from sanitizer.artifact.parsers.event_parser import EventParser
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.artifact.parsers.multi_snapshot_parser import \
    MultiSnapshotParser
from sanitizer.artifact.parsers.s3_video_parser import S3VideoParser
from sanitizer.artifact.parsers.operator_feedback_parser import OperatorFeedbackParser
from sanitizer.exceptions import ArtifactException

_logger = logging.getLogger(__name__)


@inject
class ArtifactParser:  # pylint: disable=too-few-public-methods
    """ ArtifactParser class.

    This class is responsible for parsing SQS message into artifacts.
    The artifacts are divided into two categories: video and snapshot or interior preview (GIF).
    All the video artifacts are parsed by VideoParser, their messages come from <env>-video-footage-events topic.
    All the snapshot and interior preview artifacts are parsed by SnapshotPreviewParser,
    their message comes from <env>-inputEventsTerraform.
    The RecorderTypeParser is responsible for choosing the correct parser for the artifact type.
    """

    def __init__(self,  # pylint: disable=too-many-arguments
                 s3_video_parser: S3VideoParser,
                 multi_snapshot_parser: MultiSnapshotParser,
                 event_parser: EventParser,
                 operator_feedback_parser: OperatorFeedbackParser) -> None:
        self.__s3_video_parser = s3_video_parser
        self.__multi_snapshot_parser = multi_snapshot_parser
        self.__event_parser = event_parser
        self.__operator_feedback_parser = operator_feedback_parser

    def __get_parser_for_artifact(self, artifact_type: type) -> IArtifactParser:
        """ Get parser for artifact type. """
        if issubclass(artifact_type, S3VideoArtifact):
            return self.__s3_video_parser
        if issubclass(artifact_type, MultiSnapshotArtifact):
            return self.__multi_snapshot_parser
        if issubclass(artifact_type, EventArtifact):
            return self.__event_parser
        if issubclass(artifact_type, OperatorArtifact):
            return self.__operator_feedback_parser

        raise ArtifactException(f"Artifact type not supported: {artifact_type}")

    def parse(self, sqs_message: SQSMessage) -> list[Artifact]:
        """ Parse SQS message and return list of artifacts. """
        _logger.info("parsing message into artifact...")
        artifact_type, recorder = ArtifactTypeParser.get_artifact_type_from_msg(sqs_message)
        parser = self.__get_parser_for_artifact(artifact_type)
        _logger.info("parsing artifact of type: %s", artifact_type)
        return list(parser.parse(sqs_message, recorder))
