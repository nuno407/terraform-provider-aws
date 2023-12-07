import json
import os

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import Recording, RecorderType, S3VideoArtifact, TimeWindow
from sanitizer.artifact.parsers.s3_video_parser import S3VideoParser
from helper_functions import load_sqs_json
from sanitizer.config import SanitizerConfig

MESSAGE_ID = "barfoo"
RECEIPT_HANDLE = "foobar"


@pytest.mark.unit
@pytest.mark.parametrize("test_case,input_message,expected", [
    (
        "valid_video_event",
        SQSMessage(
            message_id=MESSAGE_ID,
            receipt_handle=RECEIPT_HANDLE,
            body=load_sqs_json("valid_s3_footage_event.json"),
            timestamp="1671346291000",
            attributes=MessageAttributes(
                tenant="rubber_duck",
                device_id="srx_herbie_dev_hw_01"
            )
        ),
        [
            S3VideoArtifact(
                artifact_id="srx_herbie_dev_hw_01_InteriorRecorder_303bd782-e19f-4373-ac72-f909d62f84ce_1687178909310_1687179011058",
                tenant_id="rubber_duck",
                device_id="srx_herbie_dev_hw_01",
                recorder=RecorderType.INTERIOR,
                s3_path="s3://test-raw/rubber_duck/srx_herbie_dev_hw_01_InteriorRecorder_303bd782-e19f-4373-ac72-f909d62f84ce_1687178909310_1687179011058.mp4",
                raw_s3_path="s3://test-raw/rubber_duck/srx_herbie_dev_hw_01_InteriorRecorder_303bd782-e19f-4373-ac72-f909d62f84ce_1687178909310_1687179011058.mp4",
                anonymized_s3_path="s3://test-anonymized/rubber_duck/srx_herbie_dev_hw_01_InteriorRecorder_303bd782-e19f-4373-ac72-f909d62f84ce_1687178909310_1687179011058_anonymized.mp4",
                timestamp=1687178909310,
                end_timestamp=1687179011058,
                upload_timing=TimeWindow(
                    start=1687179039085,  # type: ignore
                    end=1687179043530),  # type: ignore
                footage_id="303bd782-e19f-4373-ac72-f909d62f84ce",
                rcc_s3_path="s3://dev-rcc-video-repo/rubber_duck/7c8c1377-38fb-4d00-af6e-b35e32b99c8a/INTERIOR/Footage_303bd782-e19f-4373-ac72-f909d62f84ce.mp4",
                recordings=[
                    Recording(
                        recording_id="InteriorRecorder-512555a5-b04d-4228-9477-3c74c29bc9de",
                        chunk_ids=[
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            9])]
            )
        ]
    )
])
def test_video_parser(test_case: str,
                      input_message: SQSMessage,
                      expected: list[S3VideoArtifact],
                      sanitizer_config: SanitizerConfig):
    print("test_case: ", test_case)
    got_video = S3VideoParser(sanitizer_config=sanitizer_config).parse(input_message, expected[0].recorder)
    assert list(got_video) == expected
