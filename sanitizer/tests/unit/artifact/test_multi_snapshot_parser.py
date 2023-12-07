""" test module for parsing snapshot artifact. """
import json
import os

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import (Artifact, MultiSnapshotArtifact,
                                  RecorderType, SnapshotArtifact, TimeWindow)
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.parsers.multi_snapshot_parser import \
    MultiSnapshotParser
from sanitizer.artifact.parsers.snapshot_parser import SnapshotParser
from helper_functions import load_sqs_json
from sanitizer.config import SanitizerConfig

MESSAGE_ID = "barfoo"
RECEIPT_HANDLE = "foobar"

snap1 = SnapshotArtifact(
    artifact_id="ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61_1685544513752",
    uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
    tenant_id="ridecare_companion_fut",
    s3_path="s3://test-raw/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61_1685544513752.jpeg",
    raw_s3_path="s3://test-raw/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61_1685544513752.jpeg",
    anonymized_s3_path="s3://test-anonymized/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61_1685544513752_anonymized.jpeg",
    timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
    end_timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
    recorder=RecorderType.INTERIOR_PREVIEW,
    upload_timing=TimeWindow(
        start="2023-05-31T14:03:51.613360+00:00",
        end="2023-05-31T15:03:51.613360+00:00"))
snap2 = SnapshotArtifact(
    artifact_id="ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62_1685544543757",
    uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
    tenant_id="ridecare_companion_fut",
    s3_path="s3://test-raw/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62_1685544543757.jpeg",
    raw_s3_path="s3://test-raw/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62_1685544543757.jpeg",
    anonymized_s3_path="s3://test-anonymized/ridecare_companion_fut/ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62_1685544543757_anonymized.jpeg",
    timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
    end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
    recorder=RecorderType.INTERIOR_PREVIEW,
    upload_timing=TimeWindow(
        start="2023-05-31T14:03:51.613360+00:00",
        end="2023-05-31T15:03:51.613360+00:00"))

multisnap = MultiSnapshotArtifact(
    artifact_id="ridecare_companion_fut_rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_1685544513752",
    tenant_id="ridecare_companion_fut",
    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
    timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
    end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
    recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
    upload_timing=TimeWindow(
        start="2023-05-31T14:03:51.613360+00:00",
        end="2023-05-31T15:03:51.613360+00:00"),
    recorder=RecorderType.INTERIOR_PREVIEW,
    chunks=[
        snap1,
        snap2])


@pytest.mark.unit
@pytest.mark.parametrize(
    "test_case,input_message,expected",
    [
        (
            "valid_preview_snapshot_event",
            SQSMessage(
                message_id=MESSAGE_ID,
                receipt_handle=RECEIPT_HANDLE,
                body=load_sqs_json("valid_preview_snapshot_event.json"),
                timestamp="1671346291000",
                attributes=MessageAttributes(
                    tenant="ridecare_companion_fut",
                    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc")
            ),
            [snap1, snap2, multisnap]
        )
    ])
def test_multi_snapshot_parser(test_case: str,
                               input_message: SQSMessage,
                               expected: list[Artifact],
                               sanitizer_config: SanitizerConfig):
    """ Test for parsing snapshot artifact. """
    print(f"test case: {test_case}")
    parser = MultiSnapshotParser(SnapshotParser(sanitizer_config=sanitizer_config))
    got_artifact = list(parser.parse(input_message, RecorderType.INTERIOR_PREVIEW))
    assert got_artifact == expected
