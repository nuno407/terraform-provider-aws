""" Integration tests for the forwarder. """
import json
from json import JSONDecodeError
from datetime import datetime

import boto3
import pytest
from kink import di
from moto import mock_sns, mock_sqs
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from pytz import UTC

from base.aws.sqs import parse_message_body_to_dict
from base.aws.sns import SNSController
from base.model.artifacts import (Artifact, MultiSnapshotArtifact,
                                  Recording, RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow, parse_artifact)
from base.timestamps import from_epoch_seconds_or_milliseconds
from sanitizer.artifact.artifact_forwarder import ArtifactForwarder

TEST_TOPIC_NAME = "test-topic"
TEST_QUEUE_NAME = "test-queue"


@pytest.fixture(scope="session")
def sqs_client() -> SQSClient:
    """Creates mocked moto3 SQS client """
    with mock_sqs():
        msqs_client = boto3.client("sqs", region_name="us-east-1")
        yield msqs_client  # NOSONAR


@pytest.fixture(scope="session")
def sns_client(sqs_client: SQSClient) -> SNSClient:  # pylint: disable=redefined-outer-name
    """Receives moto3 mocked sqs client and uses it to create
       SQS queue and subscribe this queue to a created SNS topic"""
    with mock_sns():
        queue_reponse = sqs_client.create_queue(QueueName=TEST_QUEUE_NAME)
        queue_url = queue_reponse["QueueUrl"]
        attributes = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        sqs_queue_arn = attributes["Attributes"]["QueueArn"]
        msns_client = boto3.client("sns", region_name="us-east-1")
        topic_response = msns_client.create_topic(Name=TEST_TOPIC_NAME)
        sns_topic_arn = topic_response["TopicArn"]
        di["sns_topic_arn"] = sns_topic_arn
        msns_client.subscribe(
            TopicArn=sns_topic_arn,
            Protocol="sqs",
            Endpoint=sqs_queue_arn
        )
        yield msns_client  # NOSONAR


@pytest.mark.integration
@pytest.mark.parametrize("artifact", [
    (
        SnapshotArtifact(
            uuid="test-uuid00",
            artifact_id="bar",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            recorder=RecorderType.SNAPSHOT,
            device_id="test-device-id00",
            tenant_id="test-tenant-id00",
            upload_timing=TimeWindow(
                start="2023-04-13T08:00:00+00:00",  # type: ignore
                end="2023-04-13T08:01:00+00:00")  # type: ignore

        )
    ),
    (
        S3VideoArtifact(
            footage_id="da234f8b-cfed-513a-8bbd-993aced80c93",
            artifact_id="bar",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            rcc_s3_path="s3://rcc-bucket/key",
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            recorder=RecorderType.INTERIOR,
            device_id="test-device-id01",
            tenant_id="test-tenant-id01",
            upload_timing=TimeWindow(
                start="2023-04-13T08:00:00+00:00",  # type: ignore
                end="2023-04-13T08:01:00+00:00"),  # type: ignore
            recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
        )
    ),
    (
        S3VideoArtifact(
            footage_id="f5cd9ef6-2232-5502-ac75-8fc1531f8aee",
            artifact_id="bar",
            raw_s3_path="s3://raw/foo/bar.something",
            anonymized_s3_path="s3://anonymized/foo/bar.something",
            rcc_s3_path="s3://rcc-bucket/key",
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            recorder=RecorderType.TRAINING,
            device_id="test-device-id02",
            tenant_id="test-tenant-id02",
            upload_timing=TimeWindow(
                start="2023-04-13T08:00:00+00:00",  # type: ignore
                end="2023-04-13T08:01:00+00:00"),  # type: ignore
            recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])]
        )
    ), (
        MultiSnapshotArtifact(
            tenant_id="ridecare_companion_fut",
            artifact_id="bar",
            device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
            timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
            end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
            recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
            upload_timing=TimeWindow(
                start="2023-05-31T14:03:51.613360+00:00",
                end="2023-05-31T15:03:51.613360+00:00"),
            recorder=RecorderType.INTERIOR_PREVIEW,
            chunks=[
                SnapshotArtifact(
                    uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_61.jpeg",
                    artifact_id="bar",
                    raw_s3_path="s3://raw/foo/bar.something",
                    anonymized_s3_path="s3://anonymized/foo/bar.something",
                    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
                    tenant_id="ridecare_companion_fut",
                    timestamp=from_epoch_seconds_or_milliseconds(1685544513752),
                    end_timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
                    recorder=RecorderType.INTERIOR_PREVIEW,
                    upload_timing=TimeWindow(
                        start="2023-05-31T14:03:51.613360+00:00",
                        end="2023-05-31T15:03:51.613360+00:00")),
                SnapshotArtifact(
                    uuid="InteriorRecorderPreview_InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8_62.jpeg",
                    artifact_id="bar",
                    raw_s3_path="s3://raw/foo/bar.something",
                    anonymized_s3_path="s3://anonymized/foo/bar.something",
                    device_id="rc_srx_prod_86540229e4d69c93a329000bfc8dc6b120272cbc",
                    tenant_id="ridecare_companion_fut",
                    timestamp=from_epoch_seconds_or_milliseconds(1685544543757),
                    end_timestamp=from_epoch_seconds_or_milliseconds(1685544573758),
                    recorder=RecorderType.INTERIOR_PREVIEW,
                    upload_timing=TimeWindow(
                        start="2023-05-31T14:03:51.613360+00:00",
                        end="2023-05-31T15:03:51.613360+00:00"))])
    )
])
def test_forwarder_publish_to_sns_topic(artifact: Artifact,
                                        sns_client: SNSClient,  # pylint: disable=redefined-outer-name
                                        sqs_client: SQSClient):  # pylint: disable=redefined-outer-name
    """Tests artifact encoder and decoder as well as
       publishing message to a SNS topic and receiving in
       a SQS queue using Moto3 library

        Serialized message published in SNS looks like this:
        'Message': {
            'default': {
                'tenant_id': 'test-tenant-id',
                'device_id': 'test-device-id',
                'recorder': {
                    '__enum__': 'TrainingMultiSnapshot'
                },
                'timestamp': {
                    '__datetime__': '2023-03-30T14:59:29.761066'
                },
                'uuid': 'test-uuid'
            }
        }
    """
    sns_controller = SNSController(sns_client)
    forwarder = ArtifactForwarder(sns_controller)
    forwarder.publish(artifact)
    queue_url = sqs_client.get_queue_url(QueueName=TEST_QUEUE_NAME)
    messages = sqs_client.receive_message(QueueUrl=queue_url["QueueUrl"])
    raw_body = messages["Messages"][0]["Body"]
    body = parse_message_body_to_dict(raw_body, ["Message", "Body", "default"])

    # using ArtifactDecoder to deserialize the message published in the topic
    got_artifact = parse_artifact(json.dumps(body["Message"]["default"]))
    assert got_artifact.devcloudid == artifact.devcloudid
    assert got_artifact == artifact
