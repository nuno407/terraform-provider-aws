"""test module for parsing SQS messages."""
import json
import os

import pytest

from healthcheck.exceptions import InvalidMessagePanic
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import MessageAttributes, SQSMessage

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA = os.path.join(CURRENT_LOCATION, "data")
MESSAGE_PARSER_DATA = os.path.join(TEST_DATA, "message_parser")


def _events_message_body(fixture_file_id: str) -> str:
    filepath = os.path.join(MESSAGE_PARSER_DATA, fixture_file_id)
    with open(filepath) as fp:
        return fp.read()


def _read_and_parse_msg_body_from_fixture(fixture_file_id: str) -> dict:
    raw_body = _events_message_body(fixture_file_id)
    call_args = [("'", '"'), ("\n", ""), ("\\\\", ""),
                 ("\\", ""), ('"{', "{"), ('}"', "}")]
    for args in call_args:
        raw_body = raw_body.replace(args[0], args[1])
    return json.loads(raw_body)


def _normal_events_input_message(message_id: str, recept_handle: str, fixture_file_id: str) -> dict:
    return {
        "MessageId": message_id,
        "ReceiptHandle": recept_handle,
        "MD5OfBody": "354f121d0caeedbec2e89996b4f9816d",
        "Body": _events_message_body(fixture_file_id),
        "Attributes": {
            "SentTimestamp": "1671450805019",
            "ApproximateReceiveCount": "1"
        }
    }


def _missing_receipt_handle_events_input_message(message_id: str, fixture_file_id: str) -> dict:
    return {
        "MessageId": message_id,
        "MD5OfBody": "354f121d0caeedbec2e89996b4f9816d",
        "Body": _events_message_body(fixture_file_id),
        "Attributes": {
            "SentTimestamp": "1671450805019",
            "ApproximateReceiveCount": "1"
        }
    }


def _missing_body_events_input_message(message_id: str, recept_handle: str) -> dict:
    return {
        "MessageId": message_id,
        "ReceiptHandle": recept_handle,
        "MD5OfBody": "354f121d0caeedbec2e89996b4f9816d",
        "Attributes": {
            "SentTimestamp": "1671450805019",
            "ApproximateReceiveCount": "1"
        }
    }


@pytest.mark.unit
class TestMessageParser():
    @pytest.mark.parametrize(
        "test_case,input_message,expected,is_error",
        [
            ("valid_video_footage",
             _normal_events_input_message(
                 "barfoo",
                 "foobar",
                 "valid_footage_event.json"),
                SQSMessage(
                 "barfoo",
                 "foobar",
                 "2022-12-15T16:16:32.723Z",
                 _read_and_parse_msg_body_from_fixture(
                     "valid_footage_event.json"),
                 MessageAttributes(
                     "datanauts",
                     "DATANAUTS_DEV_01")),
                False),
            ("error_footage_event_missing_timestamp",
             _normal_events_input_message(
                 "barfoo",
                 "foobar",
                 "footage_event_missing_timestamp.json"),
             {},
                True),
            ("valid_snapshot_event",
             _normal_events_input_message(
                 "barfoo",
                 "foobar",
                 "valid_snapshot_event.json"),
             SQSMessage(
                 message_id='barfoo',
                 receipt_handle='foobar',
                 timestamp='2022-12-18T07:37:07.917Z',
                 body=_read_and_parse_msg_body_from_fixture(
                     "valid_snapshot_event.json"),
                 attributes=MessageAttributes(
                     tenant='ridecare_companion_trial',
                     device_id=None)),
             False),
            ("missing_receipt_footage_event",
             _missing_receipt_handle_events_input_message(
                 "barfoo",
                 "valid_snapshot_event.json"),
             {},
             True),
            ("missing_body_footage_event",
             _missing_body_events_input_message(
                 "barfoo",
                 "valid_snapshot_event.json"),
             {},
             True)
        ])
    def test_parse_message(self, test_case: str, input_message: dict, expected: SQSMessage, is_error: bool):
        print("running test", test_case)
        if is_error:
            with pytest.raises(InvalidMessagePanic):
                SQSMessageParser().parse_message(input_message)
        else:
            got_sqs_message = SQSMessageParser().parse_message(input_message)
            assert got_sqs_message == expected
