"""Input Message"""
from dataclasses import dataclass, fields
from typing import Any

from mdfparser.constants import DataType
from mdfparser.exceptions import InvalidMessage


@dataclass
class InputMessage:
    """
    Input message
    All the messages arriving at the MDFParser queue should have this structure.
    """
    id: str  # pylint: disable=invalid-name
    s3_path: str
    data_type: DataType
    raw_s3_path: str
    tenant: str
    device_id: str
    recorder: str

    @staticmethod
    def parse_sqs_message(message_body: dict[Any, Any]) -> "InputMessage":
        """
        Parses the SQS Message body.

        Args:
            message_body (dict[Any, Any]): _description_

        Raises:
            InvalidMessage: If any InputMessage field is not in the MessageBody.

        Returns:
            InputMessage: _description_
        """

        this_fields = set(f.name for f in fields(InputMessage))
        msg_fields = set(message_body.keys())

        if not set(this_fields).issubset(msg_fields):
            raise InvalidMessage(f"Message {str(message_body)} does not contain the required fields")

        return InputMessage(
            message_body["id"],
            message_body["s3_path"],
            DataType(message_body["data_type"]),
            message_body["raw_s3_path"],
            message_body["tenant"],
            message_body["device_id"],
            message_body["recorder"])
