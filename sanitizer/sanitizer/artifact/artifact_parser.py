""" Artifact Parser module. """
from kink import inject

from base.aws.model import SQSMessage
from base.model.artifacts import Artifact, RecorderType
from sanitizer.artifact.recorder_finder import RecorderTypeFinder
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.artifact.parsers.snapshot_parser import SnapshotParser
from sanitizer.artifact.parsers.video_parser import VideoParser


@inject
class ArtifactParser:  # pylint: disable=too-few-public-methods
    """ ArtifactParser class. """

    def __init__(self,
                 video_parser: VideoParser,
                 snapshot_parser: SnapshotParser) -> None:
        self._video_parser = video_parser
        self._snapshot_parser = snapshot_parser

    def __get_parser_for_recorder(self, recorder_type: RecorderType) -> IArtifactParser:
        if recorder_type in {RecorderType.FRONT, RecorderType.INTERIOR, RecorderType.TRAINING}:
            return self._video_parser

        if recorder_type == RecorderType.SNAPSHOT:
            return self._snapshot_parser

        raise ValueError(f"Unknown recorder type: {recorder_type}")

    def parse(self, sqs_message: SQSMessage) -> list[Artifact]:
        """ Parse SQS message and return list of artifacts. """
        recorder_type = RecorderTypeFinder.get_recorder_type_from_msg(sqs_message)
        parser = self.__get_parser_for_recorder(recorder_type)
        return parser.parse(sqs_message)
