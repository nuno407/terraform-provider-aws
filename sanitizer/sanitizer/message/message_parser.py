""" SQS Message Parser module. """
import logging
from enum import Enum
from typing import Dict, Optional, Union

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.model import MessageAttributes, SQSMessage
from base.aws.sqs import parse_message_body_to_dict
from sanitizer.exceptions import InvalidMessagePanic

_logger: logging.Logger = logging.getLogger(__name__)


class MessageFields(Enum):
    """Message fields."""
    BODY_TYPE = "Type"
    MESSAGE_ID = "MessageId"
    MESSAGE = "Message"
    BODY = "Body"
    TIMESTAMP = "Timestamp"
    ATTRIBUTES = "Attributes"
    BODY_ATTRIBUTES = "MessageAttributes"
    VALUE = "value"
    RECEIPT_HANDLE = "ReceiptHandle"
    TENANT = "tenant"
    DEVICE_ID = "deviceId"
    ALTERNATIVE_DEVICE_ID = "device_id"
    PROPERTIES = "properties"
    HEADER = "header"


MANDATORY_FIELDS = [
    MessageFields.MESSAGE_ID,
    MessageFields.BODY,
    MessageFields.RECEIPT_HANDLE,
    MessageFields.ATTRIBUTES
]


@inject
class MessageParser:
    """SQS message parser."""

    def __init__(self) -> None:
        """ SQS message parser constructor. """

    @staticmethod
    def flatten_string_value(attribute: Optional[Union[str, dict]]) -> Optional[str]:
        """Unnest attribute value if is a dictionary to string value

        attribute in str format is returned imediately, attrs on dict format e.g:
        { "MyAttr": {"Value": "YourValue"} }
        are extracted

        Args:
            attribute (Union[str, dict]): message attribute in dict or str format

        Returns:
            Optional[str]: string value of the given attribute
        """
        if isinstance(attribute, str) or attribute is None:
            return attribute

        result = None
        if "Value" in attribute:
            result = attribute["Value"]
        elif "StringValue" in attribute:
            result = attribute["StringValue"]
        return result

    @staticmethod
    def get_recursive_from_dict(  # pylint: disable=dangerous-default-value
            data_dict: dict,
            *keys: str,
            default=None):
        """Get value from dict recursively."""
        for key in keys:
            if not isinstance(data_dict, Dict) or key not in data_dict:
                return default
            data_dict = data_dict[key]
        return data_dict

    def parse_message_attrs(self, body: dict) -> MessageAttributes:
        """Parses SQS message attributes

        Args:
            body (dict): parserd message body

        Returns:
            MessageAttributes: data object for message attributes
        """
        if MessageFields.BODY_ATTRIBUTES.value not in body:
            raise InvalidMessagePanic("Missing message attributes")
        message_attrs: dict = body[MessageFields.BODY_ATTRIBUTES.value]

        tenant = None
        if MessageFields.TENANT.value in message_attrs:
            tenant = MessageParser.flatten_string_value(
                message_attrs[MessageFields.TENANT.value])

        device_id = None
        if MessageFields.DEVICE_ID.value in message_attrs:
            device_id = MessageParser.flatten_string_value(
                message_attrs[MessageFields.DEVICE_ID.value])

        if device_id is None:
            value = MessageParser.get_recursive_from_dict(
                body,
                MessageFields.MESSAGE.value,
                MessageFields.VALUE.value,
                MessageFields.PROPERTIES.value,
                MessageFields.HEADER.value,
                MessageFields.ALTERNATIVE_DEVICE_ID.value)
            device_id = MessageParser.flatten_string_value(value)

        return MessageAttributes(tenant, device_id)

    def parse(self, raw_message: MessageTypeDef) -> SQSMessage:
        """Parses SQS messages into data object.

        Args:
            raw_message (str): unparsed message

        Returns:
            SQSMessage: data object for SQS message
        """
        _logger.info("Parsing message %s", raw_message)

        for field_enum in MANDATORY_FIELDS:
            if field_enum.value not in raw_message:
                raise InvalidMessagePanic(f"Missing Field {field_enum.value}")

        raw_body: str = raw_message[MessageFields.BODY.value]

        try:
            body = parse_message_body_to_dict(raw_body)
        except ValueError as exc:
            raise InvalidMessagePanic("Invalid message body.") from exc

        if not body:
            raise InvalidMessagePanic("Empty message body.")

        message_attrs = self.parse_message_attrs(body)

        receipt_handle = raw_message[MessageFields.RECEIPT_HANDLE.value]
        if not receipt_handle:
            raise InvalidMessagePanic("Receipt Handle missing.")

        timestamp = body.get(MessageFields.TIMESTAMP.value)
        if not timestamp:
            raise InvalidMessagePanic("Missing or empty message timestamp.")

        message_id = raw_message[MessageFields.MESSAGE_ID.value]
        if not message_id:
            raise InvalidMessagePanic("Missing message id.")

        return SQSMessage(
            message_id=message_id,
            receipt_handle=receipt_handle,
            body=body,
            timestamp=timestamp,
            attributes=message_attrs)
