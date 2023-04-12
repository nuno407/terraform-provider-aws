""" Unit tests for the artifact parser. """
from datetime import datetime
from unittest.mock import Mock, call

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  VideoArtifact)
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
                recorder=RecorderType.SNAPSHOT,
                device_id="DEV01",
                timestamp=datetime.now()
            ),
            SnapshotArtifact(
                uuid="foo2",
                tenant_id="datanauts",
                recorder=RecorderType.SNAPSHOT,
                device_id="DEV01",
                timestamp=datetime.now()
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
                timestamp=datetime.now()
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
            VideoArtifact(
                stream_name="baz",
                tenant_id="deepsensation",
                device_id="DEV02",
                recorder=RecorderType.INTERIOR,
                timestamp=datetime.now(),
                end_timestamp=datetime.now(),
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
            VideoArtifact(
                stream_name="baz",
                tenant_id="deepsensation",
                device_id="DEV02",
                recorder=RecorderType.TRAINING,
                timestamp=datetime.now(),
                end_timestamp=datetime.now(),
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
            VideoArtifact(
                stream_name="baz",
                tenant_id="deepsensation",
                device_id="DEV04",
                recorder=RecorderType.FRONT,
                timestamp=datetime.now(),
                end_timestamp=datetime.now(),
            )
        ]
    )
])
def test_artifact_parser(sqs_message: SQSMessage, expected_artifacts: list[Artifact]):
    snapshot_parser = Mock()
    snapshot_parser.parse = Mock(return_value=expected_artifacts)
    video_parser = Mock()
    video_parser.parse = Mock(return_value=expected_artifacts)

    artifact_parser = ArtifactParser(video_parser, snapshot_parser)
    artifacts = artifact_parser.parse(sqs_message)
    assert artifacts == expected_artifacts
    if expected_artifacts[0].recorder == RecorderType.SNAPSHOT:
        snapshot_parser.parse.assert_called_once_with(sqs_message, RecorderType.SNAPSHOT)
    elif expected_artifacts[0].recorder == RecorderType.INTERIOR_PREVIEW:
        snapshot_parser.parse.assert_called_once_with(sqs_message, RecorderType.INTERIOR_PREVIEW)
    else:
        video_parser.parse.assert_has_calls([call(sqs_message, art.recorder) for art in expected_artifacts])


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
