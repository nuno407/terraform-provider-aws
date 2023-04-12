""" Artifact Parser module. """
import logging

from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import Artifact, RecorderType
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.artifact.parsers.snapshot_preview_parser import \
    SnapshotPreviewParser
from sanitizer.artifact.parsers.video_parser import VideoParser
from sanitizer.artifact.recorder_type_parser import RecorderTypeParser
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

    def __init__(self,
                 video_parser: VideoParser,
                 snapshot_parser: SnapshotPreviewParser) -> None:
        self._video_parser = video_parser
        self._snapshot_parser = snapshot_parser

    def __get_parser_for_recorder(self, recorder_type: RecorderType) -> IArtifactParser:
        if recorder_type in {RecorderType.FRONT, RecorderType.INTERIOR, RecorderType.TRAINING}:
            return self._video_parser

        if recorder_type in {RecorderType.SNAPSHOT, RecorderType.INTERIOR_PREVIEW}:
            return self._snapshot_parser

        raise ArtifactException(f"Recorder type not supported: {recorder_type}")

    def parse(self, sqs_message: SQSMessage) -> list[Artifact]:
        """ Parse SQS message and return list of artifacts. """
        _logger.info("parsing message into artifact...")
        recorder_type = RecorderTypeParser.get_recorder_type_from_msg(sqs_message)
        parser = self.__get_parser_for_recorder(recorder_type)
        _logger.info("parsing artifact of type: %s", recorder_type.value)
        return list(parser.parse(sqs_message, recorder_type))
