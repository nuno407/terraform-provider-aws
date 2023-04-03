""" record type finder module. """
from base.aws.model import SQSMessage
from base.model.artifacts import RecorderType
from sanitizer.message.message_parser import MessageParser

# SQS message attribute recorder's values
ATTRIBUTE_INTERIOR = "INTERIOR"
ATTRIBUTE_FRONT = "FRONT"
ATTRIBUTE_TRAINING = "TRAINING"
ATTRIBUTE_SNAPSHOT = "TrainingMultiSnapshot"
ATTRIBUTE_INTERIOR_PREVIEW = "InteriorRecorderPreview"

class RecorderTypeParser:  # pylint: disable=too-few-public-methods
    """ RecorderTypeParser class. """
    @staticmethod
    def get_recorder_type_from_msg(sqs_message: SQSMessage) -> RecorderType:
        """ Get recorder type from SQS message. """
        # messages from <env>-video-footage-events topic
        recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                           .get("MessageAttributes", {})
                                                           .get("recorder", {}))
        if recorder_name == ATTRIBUTE_INTERIOR:
            return RecorderType.INTERIOR

        if recorder_name == ATTRIBUTE_FRONT:
            return RecorderType.FRONT

        if recorder_name == ATTRIBUTE_TRAINING:
            return RecorderType.TRAINING

        # messages from <env>-inputEventsTerraform
        recorder_name = MessageParser.flatten_string_value(sqs_message.body
                                                           .get("Message", {})
                                                           .get("value", {})
                                                           .get("properties", {})
                                                           .get("recorder_name", {}))
        if recorder_name == ATTRIBUTE_SNAPSHOT:
            return RecorderType.SNAPSHOT

        if recorder_name == ATTRIBUTE_INTERIOR_PREVIEW:
            return RecorderType.INTERIOR_PREVIEW

        raise ValueError(f"Unknown recorder name: {recorder_name}")
