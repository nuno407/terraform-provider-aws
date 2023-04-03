""" Message persistence module tests. """
import json
from unittest.mock import MagicMock

import pytest
from base.aws.model import SQSMessage, MessageAttributes
from sanitizer.message.message_persistence import MessagePersistence


@pytest.mark.unit
@pytest.mark.parametrize("message", [
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
            timestamp="2023-04-03T10:00:47.462",
            attributes=MessageAttributes(
                tenant="datanauts",
                device_id="DEV_01"
            )
        )
    ),
    (
        SQSMessage(
            message_id="foo2",
            receipt_handle="bar2",
            body={
                "Message": {
                    "value": {
                        "properties": {
                            "recorder_name": "TrainingMultiSnapshot"
                        }
                    }
                }
            },
            # timestamp with Z
            timestamp="2023-04-03T12:51:47.462Z",
            attributes=MessageAttributes(
                tenant="datanauts",
                device_id="DEV_01"
            )
        )
    ),
    (
        SQSMessage(
            message_id="foo2",
            receipt_handle="bar2",
            body={
                "Message": {
                    "value": {
                        "properties": {
                            "recorder_name": "TrainingMultiSnapshot"
                        }
                    }
                }
            },
            # timestamp without Z and with UTC offset
            timestamp="2023-04-03T12:51:47.462+00:00",
            attributes=MessageAttributes(
                tenant="datanauts",
                device_id="DEV_01"
            )
        )
    )
])
def test_save(message: SQSMessage):
    """ Test save method. """
    mongo_client = MagicMock()
    database = MagicMock()
    collection = MagicMock()
    database.__getitem__.return_value = collection
    mongo_client.__getitem__.return_value = database
    config = MagicMock()

    persistence = MessagePersistence(mongo_client, config)
    persistence.save(message)

    collection.insert_one.assert_called_once_with(persistence.to_persistable_dict(message))
