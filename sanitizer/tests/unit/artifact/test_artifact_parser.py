""" Unit tests for the artifact parser. """
from datetime import datetime
from unittest.mock import Mock, call

import pytest
from pytz import UTC

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import (Artifact, RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from sanitizer.artifact.artifact_parser import ArtifactParser


@pytest.mark.unit
@pytest.mark.parametrize("sqs_message,expected_artifacts", [
    # snapshot
    (
        SQSMessage(
            message_id="foo",
            receipt_handle="bar",
            body={
                "TopicArn": "arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform",
                "Message": {
                    "value": {
                        "properties": {
                            "recorder_name": "TrainingMultiSnapshot"
                        }
                    },
                    "timestamp": "2022-12-18T07:37:07.842030994Z"
                }
            },
            timestamp="123456",
            attributes=MessageAttributes(
                tenant="datanauts",
                device_id="DEV01")),
        [
            SnapshotArtifact(
                uuid="foo1",
                tenant_id="datanauts",
                recorder=RecorderType.SNAPSHOT,
                device_id="DEV01",
                timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            ),
            SnapshotArtifact(
                uuid="foo2",
                tenant_id="datanauts",
                recorder=RecorderType.SNAPSHOT,
                device_id="DEV01",
                timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            )
        ]
    ),
    # # interior preview
    (
        SQSMessage(
            message_id="foo",
            receipt_handle="bar",
            body={
                "TopicArn": "arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform",
                "Message": {
                    "value": {
                        "properties": {
                            "recorder_name": "InteriorRecorderPreview"
                        }
                    }
                }
            },
            timestamp="123456",
            attributes=MessageAttributes(
                tenant="datanauts",
                device_id="DEV01")),
        [
            SnapshotArtifact(
                uuid="foo1",
                tenant_id="datanauts",
                recorder=RecorderType.INTERIOR_PREVIEW,
                device_id="DEV01",
                timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            )
        ]
    ),
    # interior recorder
    (
        SQSMessage(
            message_id="foo",
            receipt_handle="bar",
            body={
                "TopicArn": "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",
                "MessageAttributes": {
                    "recorder": {
                        "Type": "String",
                        "Value": "INTERIOR"
                    }
                }
            },
            timestamp="123456",
            attributes=MessageAttributes(
                tenant="deepsensation",
                device_id="DEV02")),
        [
            S3VideoArtifact(
                footage_id="88625bd7-6d6f-551c-a7ae-2ed13fbaa2bf",
                rcc_s3_path="s3://rcc-bucket/key",
                tenant_id="deepsensation",
                device_id="DEV02",
                recorder=RecorderType.INTERIOR,
                timestamp=datetime.now(tz=UTC),
                end_timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            )
        ]
    ),
    # training recorder
    (
        SQSMessage(
            message_id="bar",
            receipt_handle="foobar",
            body={
                "TopicArn": "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",
                "MessageAttributes": {
                    "recorder": {
                        "Type": "String",
                        "Value": "TRAINING"
                    }
                }
            },
            timestamp="123456789",
            attributes=MessageAttributes(
                tenant="goaldiggers",
                device_id="DEV03")),
        [
            S3VideoArtifact(
                footage_id="c2dd5a8d-6b23-5908-99de-483efef5cf28",
                rcc_s3_path="s3://rcc-bucket/key",
                tenant_id="deepsensation",
                device_id="DEV02",
                recorder=RecorderType.TRAINING,
                timestamp=datetime.now(tz=UTC),
                end_timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            )
        ]
    ),
    # front recorder
    (
        SQSMessage(
            message_id="bar",
            receipt_handle="foobar",
            body={
                "TopicArn": "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",
                "MessageAttributes": {
                    "recorder": {
                        "Type": "String",
                        "Value": "FRONT"
                    }
                }
            },
            timestamp="123456789",
            attributes=MessageAttributes(
                tenant="goaldiggers",
                device_id="DEV04")),
        [
            S3VideoArtifact(
                footage_id="2a59bb30-4c74-557f-8de9-0ce1a031d910",
                rcc_s3_path="s3://rcc-bucket/key",
                tenant_id="deepsensation",
                device_id="DEV04",
                recorder=RecorderType.FRONT,
                timestamp=datetime.now(tz=UTC),
                end_timestamp=datetime.now(tz=UTC),
                upload_timing=TimeWindow(
                    start="2022-12-18T07:37:07.842030994Z",
                    end="2022-12-18T07:37:07.842030994Z")
            )
        ]
    )
])
def test_artifact_parser(sqs_message: SQSMessage, expected_artifacts: list[Artifact]):
    snapshot_parser = Mock()
    snapshot_parser.parse = Mock(return_value=expected_artifacts)
    kinesis_video_parser = Mock()
    kinesis_video_parser.parse = Mock(return_value=expected_artifacts)
    s3_video_parser = Mock()
    s3_video_parser.parse = Mock(return_value=expected_artifacts)

    artifact_parser = ArtifactParser(kinesis_video_parser, s3_video_parser, snapshot_parser)
    artifacts = artifact_parser.parse(sqs_message)
    assert artifacts == expected_artifacts
    if expected_artifacts[0].recorder == RecorderType.SNAPSHOT:
        snapshot_parser.parse.assert_called_once_with(sqs_message, RecorderType.SNAPSHOT)
    elif expected_artifacts[0].recorder == RecorderType.INTERIOR_PREVIEW:
        snapshot_parser.parse.assert_called_once_with(sqs_message, RecorderType.INTERIOR_PREVIEW)
    else:
        kinesis_video_parser.parse.assert_has_calls([call(sqs_message, art.recorder) for art in expected_artifacts])


def test_artifact_parser_unknown_recorder():
    sqs_message = SQSMessage(
        message_id="bar",
        receipt_handle="foobar",
        body={
            "MessageAttributes": {
                "recorder": {
                    "Type": "String",
                    "Value": "FOO"
                }
            }
        },
        timestamp="123456789",
        attributes=MessageAttributes(
            tenant="goaldiggers",
            device_id="DEV03"))

    snapshot_parser = Mock()
    snapshot_parser.parse = Mock(return_value=[])
    video_parser = Mock()
    video_parser.parse = Mock(return_value=[])

    artifact_parser = ArtifactParser(video_parser, snapshot_parser)
    with pytest.raises(ValueError):
        artifact_parser.parse(sqs_message)
