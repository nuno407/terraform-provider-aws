""" Persistence integration tests module. """

import pytest
from mongomock import MongoClient

from base.aws.model import SQSMessage, MessageAttributes
from sanitizer.config import SanitizerConfig
from sanitizer.message.message_persistence import MessagePersistence


@pytest.mark.integration
@pytest.mark.parametrize("sqs_message", [
    (
        SQSMessage(
            message_id="foo1",
            receipt_handle="bar",
            body={
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
                device_id="DEV_01"))
    ),
    (
        SQSMessage(
            message_id="baz2",
            receipt_handle="foo",
            body={
                "MessageAttributes": {
                    "recorder": {
                        "Type": "String",
                        "Value": "INTERIOR"
                    }
                }
            },
            timestamp="1234567890",
            attributes=MessageAttributes(
                tenant="rubberduck",
                device_id="DEV_02"))
    )
])
def test_integration_persistence(sqs_message: SQSMessage):
    mocked_client = MongoClient()
    db_name = "data-ingestion-db"
    message_collection = "message-collection"
    config = SanitizerConfig(
        input_queue="foo",
        topic_arn="bar",
        db_name=db_name,
        message_collection=message_collection,
        tenant_blacklist=[],
        training_whitelist=[],
        recorder_blacklist=[]
    )

    persistence = MessagePersistence(
        mongo_client=mocked_client,
        config=config)

    persistence.save(sqs_message)

    found = mocked_client[db_name][message_collection].find_one({
        "message_id": sqs_message.message_id
    })

    assert found["receipt_handle"] == sqs_message.receipt_handle
    assert found["timestamp"] == sqs_message.timestamp
    assert found["body"] == sqs_message.body
    assert found["attributes"]["tenant"] == sqs_message.attributes.tenant
    assert found["attributes"]["device_id"] == sqs_message.attributes.device_id
