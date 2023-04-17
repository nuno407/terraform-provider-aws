""" test module for parsing snapshot artifact. """
import os
import json
import pytest

from base.aws.model import SQSMessage, MessageAttributes
from base.model.artifacts import RecorderType, SnapshotArtifact, Artifact
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.snapshot_preview_parser import SnapshotPreviewParser

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "..", "data")
MESSAGE_PARSER_DATA = os.path.join(TEST_DATA, "message_parser")


def _events_message_raw_body(fixture_file_id: str) -> str:
    filepath = os.path.join(MESSAGE_PARSER_DATA, fixture_file_id)
    with open(filepath, encoding="utf-8") as fp:
        return fp.read()


def _valid_events_message_body(fixture_file_id: str) -> dict:
    raw_body = _events_message_raw_body(fixture_file_id)
    return json.loads(raw_body)


MESSAGE_ID = "barfoo"
RECEIPT_HANDLE = "foobar"


@pytest.mark.unit
@pytest.mark.parametrize("test_case,input_message,expected", [
    (
        "valid_snapshot_event",
        SQSMessage(
            message_id=MESSAGE_ID,
            receipt_handle=RECEIPT_HANDLE,
            body=_valid_events_message_body("valid_snapshot_event.json"),
            timestamp="1671346291000",
            attributes=MessageAttributes(
                tenant="ridecare_companion_trial",
                device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b"
            )),
        [
            SnapshotArtifact(
                uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_342.jpeg",
                device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b",
                tenant_id="ridecare_companion_trial",
                timestamp=from_epoch_seconds_or_milliseconds(1671346291000),
                recorder=RecorderType.SNAPSHOT),
            SnapshotArtifact(
                uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-a574c99e-9841-40a9-ab8b-ebbe789ae631_369.jpeg",
                device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b",
                tenant_id="ridecare_companion_trial",
                timestamp=from_epoch_seconds_or_milliseconds(1671347823000),
                recorder=RecorderType.SNAPSHOT)
        ],
    )
])
def test_snapshot_parser(test_case: str,
                         input_message: SQSMessage,
                         expected: Artifact):
    """ Test for parsing snapshot artifact. """
    print(f"test case: {test_case}")
    got_artifact = SnapshotPreviewParser().parse(input_message, RecorderType.SNAPSHOT)
    assert list(got_artifact) == expected
