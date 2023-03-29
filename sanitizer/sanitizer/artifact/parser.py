""" Artifact Parser module. """
from kink import inject

from base.model.artifacts import Artifact, RecorderType
from base.aws.model import SQSMessage
from sanitizer.artifact.parsers.video_parser import VideoParser
from sanitizer.artifact.parsers.snapshot_parser import SnapshotParser
from sanitizer.message.parser import MessageParser
flatten_string_value = MessageParser.flatten_string_value


@inject
class ArtifactParser:
    def __init__(self, video_parser: VideoParser, snapshot_parser: SnapshotParser) -> None:
        self._video_parser = video_parser
        self._snapshot_parser = snapshot_parser

    def __get_recorder_type_from_msg(self, sqs_message: SQSMessage) -> RecorderType:
        recorder_name = flatten_string_value(sqs_message.body.get("MessageAttributes", {})\
                                             .get("recorder", {}))
        if recorder_name == "INTERIOR":
            return RecorderType.INTERIOR
        elif recorder_name == "FRONT":
            return RecorderType.FRONT
        elif recorder_name == "TRAINING":
            return RecorderType.TRAINING

        recorder_name = flatten_string_value(sqs_message.body.get("Message", {})\
            .get("value", {})\
                .get("properties", {})\
                    .get("recorder", {}))
        if recorder_name == "TrainingMultiSnapshot":
            return RecorderType.SNAPSHOT

    def __get_parser_for_recorder(self, recorder_type: RecorderType) -> ArtifactParser:
        if recorder_type == RecorderType.FRONT or\
           recorder_type == RecorderType.INTERIOR or\
           recorder_type == RecorderType.TRAINING:
            return self._video_parser
        elif recorder_type == RecorderType.SNAPSHOT:
            return self._snapshot_parser


    def parse(self, sqs_message: SQSMessage) -> list[Artifact]:
        recorder_type = self.__get_recorder_type_from_msg(sqs_message)
        parser = self.__get_parser_for_recorder(recorder_type)
        return parser.parse(sqs_message)
