import json
import os

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import KinesisVideoArtifact, RecorderType, TimeWindow
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.kinesis_video_parser import KinesisVideoParser

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
        "valid_video_event",
        SQSMessage(
            message_id=MESSAGE_ID,
            receipt_handle=RECEIPT_HANDLE,
            body=_valid_events_message_body("valid_footage_event.json"),
            timestamp="1671346291000",
            attributes=MessageAttributes(
                tenant="ridecare_companion_trial",
                device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b"
            )
        ),
        [
            KinesisVideoArtifact(
                tenant_id="ridecare_companion_trial",
                device_id="rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b",
                recorder=RecorderType.INTERIOR,
                stream_name="DATANAUTS_DEV_01_InteriorRecorder",
                timestamp=from_epoch_seconds_or_milliseconds(1671118349783),
                end_timestamp=from_epoch_seconds_or_milliseconds(1671120149783),
                upload_timing=TimeWindow(
                    start=1671118695636,  # type: ignore
                    end=1671120664474),  # type: ignore
            )
        ]
    )
])
def test_video_parser(test_case: str,
                      input_message: SQSMessage,
                      expected: list[KinesisVideoArtifact]):
    print("test_case: ", test_case)
    got_video = KinesisVideoParser().parse(input_message, expected[0].recorder)
    assert list(got_video) == expected
