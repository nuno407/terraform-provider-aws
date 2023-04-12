""" Integration tests for the forwarder. """
import json
from datetime import datetime

import boto3
import pytest
from kink import di
from moto import mock_sns, mock_sqs
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient

from base.aws.sns import SNSController
from base.model.artifacts import (Artifact, ArtifactDecoder, RecorderType,
                                  SnapshotArtifact, VideoArtifact)
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


def _read_and_parse_msg_body_from_sns_topic(raw_body: str) -> dict:
    call_args = [("'", '"'), ("\n", ""), ("\\\\", ""),  # pylint: disable=invalid-string-quote
                 ("\\", ""), ('"{', "{"), ('}"', "}")]  # pylint: disable=invalid-string-quote
    for args in call_args:
        raw_body = raw_body.replace(args[0], args[1])
    return json.loads(raw_body)


@pytest.mark.integration
@pytest.mark.parametrize("artifact", [
    (
        SnapshotArtifact(
            uuid="test-uuid00",
            timestamp=datetime.now(),
            recorder=RecorderType.SNAPSHOT,
            device_id="test-device-id00",
            tenant_id="test-tenant-id00",
        )
    ),
    (
        VideoArtifact(
            stream_name="test-stream-name01",
            timestamp=datetime.now(),
            end_timestamp=datetime.now(),
            recorder=RecorderType.INTERIOR,
            device_id="test-device-id01",
            tenant_id="test-tenant-id01",
        )
    ),
    (
        VideoArtifact(
            stream_name="test-training-stream-name02",
            timestamp=datetime.now(),
            end_timestamp=datetime.now(),
            recorder=RecorderType.TRAINING,
            device_id="test-device-id02",
            tenant_id="test-tenant-id02",
        )
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
    body = _read_and_parse_msg_body_from_sns_topic(raw_body)

    # using ArtifactDecoder to deserialize the message published in the topic
    got_artifact = json.loads(json.dumps(body["Message"]["default"]), cls=ArtifactDecoder)
    assert got_artifact.devcloudid == artifact.devcloudid
    assert got_artifact == artifact
