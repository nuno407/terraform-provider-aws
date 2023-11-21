"""test module for parsing SQS messages."""
import json
import os

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from sanitizer.exceptions import InvalidMessagePanic
from sanitizer.message.message_parser import MessageParser
from typing import Union, Optional

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "..", "data")
MESSAGE_PARSER_DATA = os.path.join(TEST_DATA, "message_parser")


def _events_message_body(fixture_file_id: str) -> str:
    filepath = os.path.join(MESSAGE_PARSER_DATA, fixture_file_id)
    with open(filepath, encoding="utf-8") as fp:
        return fp.read()


def _read_and_parse_msg_body_from_fixture(fixture_file_id: str) -> dict:
    raw_body = _events_message_body(fixture_file_id)
    json_body = json.loads(raw_body)
    if "Message" in json_body and isinstance(json_body["Message"], str):
        json_body["Message"] = json.loads(json_body["Message"])
    return json_body


MESSAGE_ID = "barfoo"
RECEIPT_HANDLE = "foobar"


def _valid_input_message(fixture_file_id: str) -> dict:
    return {
        "MessageId": MESSAGE_ID,
        "ReceiptHandle": RECEIPT_HANDLE,
        "MD5OfBody": "354f121d0caeedbec2e89996b4f9816d",
        "Body": _events_message_body(fixture_file_id),
        "Attributes": {
            "SentTimestamp": "1671450805019",
            "ApproximateReceiveCount": "1"
        }
    }


def _missing_receipt_handle_message(fixture_file_id: str) -> dict:
    message = _valid_input_message(fixture_file_id)
    message.pop("ReceiptHandle")
    return message


def _missing_body_message(fixture_file_id: str) -> dict:
    message = _valid_input_message(fixture_file_id)
    message.pop("Body")
    return message


@pytest.mark.unit
class TestMessageParser():  # pylint: disable=too-few-public-methods
    """ Test class for MessageParser. """
    @pytest.mark.parametrize("test_case,input_message,expected,is_error",
                             [("error_footage_event_missing_timestamp",
                               _valid_input_message("footage_event_missing_timestamp.json"),
                                 {},
                                 True),
                                 ("valid_snapshot_event",
                                  _valid_input_message("valid_snapshot_event.json"),
                                  SQSMessage(MESSAGE_ID,
                                             RECEIPT_HANDLE,
                                             timestamp='2022-12-18T07:37:07.917Z',
                                             body=_read_and_parse_msg_body_from_fixture("valid_snapshot_event.json"),
                                             attributes=MessageAttributes(tenant='ridecare_companion_trial',
                                                                          device_id='rc_srx_prod_5c88ed5d1a39500838867f5fd03f8017d295250b')),
                                  False),
                                 ("valid_event_containing_apostrophe",
                                  _valid_input_message("valid_special_event.json"),
                                  SQSMessage(MESSAGE_ID,
                                             RECEIPT_HANDLE,
                                             timestamp='2023-07-25T08:12:22.523Z',
                                             body=_read_and_parse_msg_body_from_fixture("valid_special_event.json"),
                                             attributes=MessageAttributes(tenant="herbie",
                                                                          device_id="srx_herbie_dev_hw_02")),
                                  False),
                                 ("valid_camera_service_event_no_device",
                                  _valid_input_message("valid_camera_service_event_no_device.json"),
                                  {},
                                  True),
                                 ("missing_receipt_footage_event",
                                  _missing_receipt_handle_message("valid_snapshot_event.json"),
                                  {},
                                  True),
                                 ("missing_body_footage_event",
                                  _missing_body_message("valid_snapshot_event.json"),
                                  {},
                                  True),
                                 ("invalid_body_footage_event",
                                  _valid_input_message("broken_footage_event.json"),
                                  {},
                                  True)])
    def test_parse_message(self, test_case: str, input_message: dict,
                           expected: SQSMessage, is_error: bool):
        if is_error:
            with pytest.raises(InvalidMessagePanic):
                MessageParser().parse(input_message)
        else:
            got_sqs_message = MessageParser().parse(input_message)
            assert got_sqs_message == expected

    @pytest.mark.parametrize("attribute,expected_result", [
        (
            {"Value": "10"},
            "10"
        ),
        (
            {"StringValue": "test"},
            "test"
        ),
        (
            {"Nothing", "test"},
            None
        ),
        (
            "some_value",
            "some_value"
        ),
        (
            None,
            None
        )
    ])
    def test_flatten_string_value(
            self, attribute: Union[str, dict], expected_result: Optional[str]):
        value = MessageParser().flatten_string_value(attribute)
        assert value == expected_result
