""" record type finder module. """
from typing import Optional, Tuple

from base.aws.model import SQSMessage
from base.model.artifacts import (EventArtifact, KinesisVideoArtifact,
                                  MultiSnapshotArtifact, RecorderType,
                                  S3VideoArtifact)
from sanitizer.exceptions import ArtifactException, MessageException
from sanitizer.message.message_parser import MessageParser

# SQS message attribute recorder's values
RECORDER_TYPE_MAP = {
    "INTERIOR": RecorderType.INTERIOR,
    "FRONT": RecorderType.FRONT,
    "TRAINING": RecorderType.TRAINING,
    "TrainingMultiSnapshot": RecorderType.SNAPSHOT,
    "InteriorRecorderPreview": RecorderType.INTERIOR_PREVIEW
}

ALLOWED_RECORDERS = {
    S3VideoArtifact: [
        RecorderType.FRONT,
        RecorderType.INTERIOR,
        RecorderType.TRAINING
    ],
    KinesisVideoArtifact: [
        RecorderType.FRONT,
        RecorderType.INTERIOR,
        RecorderType.TRAINING
    ],
    MultiSnapshotArtifact: [
        RecorderType.INTERIOR_PREVIEW,
        RecorderType.SNAPSHOT
    ],
    EventArtifact: [
        None
    ]
}


class ArtifactTypeParser:  # pylint: disable=too-few-public-methods
    """ RecorderTypeParser class. """
    @staticmethod
    def _parse_recorder_value(recorder_value: Optional[str]) -> Optional[RecorderType]:
        if recorder_value is None:
            return None
        if recorder_value in RECORDER_TYPE_MAP:
            return RECORDER_TYPE_MAP[recorder_value]
        if recorder_value in [e.value for e in RecorderType]:
            return [e for e in RecorderType if e.value == recorder_value][0]
        raise MessageException(f"Cannot extract recorder type from value: {recorder_value}")

    @staticmethod
    def _check_recorder_is_allowed_for_artifact(artifact_type: type, recorder: RecorderType):
        if recorder not in ALLOWED_RECORDERS[artifact_type]:
            raise ArtifactException("Message has an invalid combination of recorder and artifact type.")

    @staticmethod
    def get_artifact_type_from_msg(sqs_message: SQSMessage) -> Tuple[type, Optional[RecorderType]]:
        """ Get recorder type from SQS message. """
        topic_arn = sqs_message.body.get("TopicArn", "MISSINGTOPIC")
        artifact_type = None
        recorder_name = None
        recorder = None

        if topic_arn.endswith("video-footage-events"):
            if "uploadInfos" in sqs_message.body.get("Message", {}):
                artifact_type = S3VideoArtifact
            else:
                artifact_type = KinesisVideoArtifact

            recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                               .get("MessageAttributes", {})
                                                               .get("recorder", {}))
        elif topic_arn.endswith("inputEventsTerraform"):
            rcc_event_name = MessageParser.get_recursive_from_dict(sqs_message.body,
                                                                   "Message",
                                                                   "value",
                                                                   "properties",
                                                                   "header",
                                                                   "message_type")
            rcc_event_name = MessageParser.flatten_string_value(rcc_event_name)

            if rcc_event_name == "com.bosch.ivs.videorecorder.UploadRecordingEvent":
                artifact_type = MultiSnapshotArtifact
                recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                                   .get("Message", {})
                                                                   .get("value", {})
                                                                   .get("properties", {})
                                                                   .get("recorder_name", {}))
            else:
                artifact_type = EventArtifact

        if artifact_type is None:
            raise MessageException(f"Cannot extract artifact type from message: {sqs_message}")
        recorder = ArtifactTypeParser._parse_recorder_value(recorder_name)
        ArtifactTypeParser._check_recorder_is_allowed_for_artifact(artifact_type, recorder)
        return artifact_type, recorder
