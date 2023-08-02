""" record type finder module. """
from typing import Optional, Tuple

from base.aws.model import SQSMessage
from base.model.artifacts import (EventArtifact, KinesisVideoArtifact,
                                  MultiSnapshotArtifact, RecorderType,
                                  S3VideoArtifact)
from sanitizer.exceptions import MessageException
from sanitizer.message.message_parser import MessageParser

# SQS message attribute recorder's values
RECORDER_TYPE_MAP = {
    "INTERIOR": RecorderType.INTERIOR,
    "FRONT": RecorderType.FRONT,
    "TRAINING": RecorderType.TRAINING,
    "TrainingMultiSnapshot": RecorderType.SNAPSHOT,
    "InteriorRecorderPreview": RecorderType.INTERIOR_PREVIEW
}


class ArtifactTypeParser:  # pylint: disable=too-few-public-methods
    """ RecorderTypeParser class. """
    @staticmethod
    def get_artifact_type_from_msg(sqs_message: SQSMessage) -> Tuple[type, Optional[RecorderType]]:
        """ Get recorder type from SQS message. """
        topic_arn = sqs_message.body.get("TopicArn", "MISSINGTOPIC")

        if topic_arn.endswith("video-footage-events"):
            if "uploadInfos" in sqs_message.body.get("Message", {}):
                artifact_type = S3VideoArtifact
            else:
                artifact_type = KinesisVideoArtifact

            recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                               .get("MessageAttributes", {})
                                                               .get("recorder", {}))
            if recorder_name not in RECORDER_TYPE_MAP:
                raise MessageException(f"Cannot extract recorder type from message: {sqs_message}")
            return artifact_type, RECORDER_TYPE_MAP[recorder_name]

        if topic_arn.endswith("inputEventsTerraform"):
            rcc_event_name = MessageParser.get_recursive_from_dict(sqs_message.body,
                                                                   "Message",
                                                                   "value",
                                                                   "properties",
                                                                   "header",
                                                                   "message_type")
            rcc_event_name = MessageParser.flatten_string_value(rcc_event_name)

            if rcc_event_name == "com.bosch.ivs.videorecorder.UploadRecordingEvent":
                recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                                   .get("Message", {})
                                                                   .get("value", {})
                                                                   .get("properties", {})
                                                                   .get("recorder_name", {}))
                if recorder_name not in RECORDER_TYPE_MAP:
                    raise MessageException(f"Cannot extract recorder type from message: {sqs_message}")
                return MultiSnapshotArtifact, RECORDER_TYPE_MAP[recorder_name]

            return EventArtifact, None

        raise MessageException(f"Cannot extract artifact type from message: {sqs_message}")
