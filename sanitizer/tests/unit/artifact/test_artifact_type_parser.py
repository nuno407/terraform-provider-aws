import json
import os

import pytest

from base.aws.model import SQSMessage
from base.model.artifacts import (EventArtifact, MultiSnapshotArtifact, RecorderType,
                                  S3VideoArtifact)
from sanitizer.artifact.artifact_type_parser import (ALLOWED_RECORDERS,
                                                     ArtifactTypeParser)
from sanitizer.exceptions import ArtifactException, MessageException

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "..", "data", "message_parser")

TEST_FILES_AND_TYPES = [
    ("valid_preview_snapshot_event.json", MultiSnapshotArtifact),
    ("valid_s3_footage_event.json", S3VideoArtifact),
    ("valid_snapshot_event.json", MultiSnapshotArtifact),
    ("valid_incident_event.json", EventArtifact)
]

RECORDERS = {
    ("INTERIOR", RecorderType.INTERIOR),
    ("InteriorRecorder", RecorderType.INTERIOR),
    ("FRONT", RecorderType.FRONT),
    ("FrontRecorder", RecorderType.FRONT),
    ("TRAINING", RecorderType.TRAINING),
    ("TrainingRecorder", RecorderType.TRAINING),
    ("TrainingMultiSnapshot", RecorderType.SNAPSHOT),
    ("InteriorRecorderPreview", RecorderType.INTERIOR_PREVIEW)
}


class TestArtifactTypeParser:
    def prepare_sqs_message(self, test_file: str, recorder_to_set: str) -> tuple[SQSMessage, bool]:
        # 1. prepare test data
        with open(os.path.join(TEST_DATA, test_file)) as f:
            message_body = json.loads(f.read())
            if "Body" in message_body:
                message_body = message_body["Body"]
        # parse into pure dict format if neccessary
        if isinstance(message_body, str):
            message_body["Message"] = json.loads(message_body["Message"])
        # 2. replace recorder attribute with the one from our test, only if present
        recorder_replaced = False
        if "MessageAttributes" in message_body and "recorder" in message_body["MessageAttributes"]:
            message_body["MessageAttributes"]["recorder"] = recorder_to_set
            recorder_replaced = True
        elif "value" in message_body["Message"] and "properties" in message_body["Message"]["value"] and "recorder_name" in message_body["Message"]["value"]["properties"]:
            message_body["Message"]["value"]["properties"]["recorder_name"] = recorder_to_set
            recorder_replaced = True
        # 3. prepare SQSMessage object
        return SQSMessage(None, None, None, message_body, None), recorder_replaced

    @pytest.mark.unit
    @pytest.mark.parametrize("test_file,expected_type", TEST_FILES_AND_TYPES)
    @pytest.mark.parametrize("recorder_attribute_value,expected_recorder", RECORDERS)
    def test_artifact_type_parser(self,
                                  test_file: str,
                                  recorder_attribute_value: str,
                                  expected_type: type,
                                  expected_recorder: RecorderType):
        # GIVEN
        sqs_message, recorder_replaced = self.prepare_sqs_message(
            test_file, recorder_attribute_value)

        # WHEN
        if not recorder_replaced:
            expected_recorder = None
        if expected_recorder in ALLOWED_RECORDERS[expected_type]:
            art_type, recorder = ArtifactTypeParser.get_artifact_type_from_msg(sqs_message)

            # THEN
            assert art_type == expected_type
            assert recorder == expected_recorder

        else:
            with pytest.raises(ArtifactException):
                ArtifactTypeParser.get_artifact_type_from_msg(sqs_message)

    @pytest.mark.unit
    @pytest.mark.parametrize("test_file,expected_type", TEST_FILES_AND_TYPES)
    def test_parsing_fails_for_unknown_recorders(self,
                                                 test_file: str,
                                                 expected_type: type):
        # GIVEN
        sqs_message, recorder_replaced = self.prepare_sqs_message(test_file, "foo_recorder")

        # WHEN - THEN
        if recorder_replaced:
            with pytest.raises(MessageException):
                ArtifactTypeParser.get_artifact_type_from_msg(sqs_message)