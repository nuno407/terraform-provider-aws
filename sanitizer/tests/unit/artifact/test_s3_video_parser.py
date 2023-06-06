import json
import os

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import RecorderType, S3VideoArtifact, TimeWindow
from sanitizer.artifact.parsers.s3_video_parser import S3VideoParser

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
            body=_valid_events_message_body("valid_s3_footage_event.json"),
            timestamp="1671346291000",
            attributes=MessageAttributes(
                tenant="rubber_duck",
                device_id="srx_herbie_dev_hw_01"
            )
        ),
        [
            S3VideoArtifact(
                tenant_id="rubber_duck",
                device_id="srx_herbie_dev_hw_01",
                recorder=RecorderType.INTERIOR,
                timestamp=1687178909310,
                end_timestamp=1687179011058,
                upload_timing=TimeWindow(
                    start=1687179039085,  # type: ignore
                    end=1687179043530),  # type: ignore
                footage_id="303bd782-e19f-4373-ac72-f909d62f84ce",
                rcc_s3_path="s3://dev-rcc-video-repo/rubber_duck/7c8c1377-38fb-4d00-af6e-b35e32b99c8a/INTERIOR/Footage_303bd782-e19f-4373-ac72-f909d62f84ce.mp4"
            )
        ]
    )
])
def test_video_parser(test_case: str,
                      input_message: SQSMessage,
                      expected: list[S3VideoArtifact]):
    print("test_case: ", test_case)
    got_video = S3VideoParser().parse(input_message, expected[0].recorder)
    assert list(got_video) == expected
