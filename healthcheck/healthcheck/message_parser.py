"""SQS Message Parser module."""
import json
import logging
from enum import Enum
from typing import Optional, Union
from mypy_boto3_sqs.type_defs import MessageTypeDef

from healthcheck.exceptions import InvalidMessageError
from healthcheck.model import MessageAttributes, SQSMessage

_logger: logging.Logger = logging.getLogger(__name__)


class MessageFields(Enum):
    """Message fields."""
    BODY_TYPE = "Type"
    MESSAGE_ID = "MessageId"
    BODY = "Body"
    TIMESTAMP = "Timestamp"
    ATTRIBUTES = "Attributes"
    BODY_ATTRIBUTES = "MessageAttributes"
    RECEIPT_HANDLE = "ReceiptHandle"
    TENANT = "tenant"
    DEVICE_ID = "deviceId"


MANDATORY_FIELDS = [
    MessageFields.MESSAGE_ID,
    MessageFields.BODY,
    MessageFields.RECEIPT_HANDLE,
    MessageFields.ATTRIBUTES]


class SQSMessageParser():
    """SQS message parser."""

    def __flatten_string_value(self, attribute: Union[str, dict]) -> Optional[str]:
        """Unnest attribute value if is a dictionary to string value

        attribute in str format is returned imediately, attrs on dict format e.g:
        { "MyAttr": {"Value": "YourValue"} }
        are extracted

        Args:
            attribute (Union[str, dict]): message attribute in dict or str format

        Returns:
            Optional[str]: string value of the given attribute
        """
        if isinstance(attribute, str):
            return attribute

        result = None
        if "Value" in attribute:
            result = attribute["Value"]
        elif "StringValue" in attribute:
            result = attribute["StringValue"]
        return result

    def parse_message_attrs(self, body: dict) -> MessageAttributes:
        """Parses SQS message attributes

        Args:
            body (dict): parserd message body

        Returns:
            MessageAttributes: data object for message attributes
        """
        if not MessageFields.BODY_ATTRIBUTES.value in body:
            raise InvalidMessageError("Missing message attributes")
        message_attrs: dict = body[MessageFields.BODY_ATTRIBUTES.value]

        tenant = None
        if MessageFields.TENANT.value in message_attrs:
            tenant = self.__flatten_string_value(message_attrs[MessageFields.TENANT.value])

        device_id = None
        if MessageFields.DEVICE_ID.value in message_attrs:
            device_id = self.__flatten_string_value(message_attrs[MessageFields.DEVICE_ID.value])

        return MessageAttributes(tenant, device_id)

    def __deserialize(self, raw_message: str) -> str:
        call_args = [("'", '"'), ("\\n", ""), ("\\\\", ""), ("\\", ""), ('"{', "{"), ('}"', "}")]
        for args in call_args:
            raw_message = raw_message.replace(args[0], args[1])
        return raw_message

    def parse_message(self, raw_message: MessageTypeDef) -> SQSMessage:
        """Parses SQS messages into data object.

        Args:
            raw_message (str): unparsed message

        Returns:
            SQSMessage: data object for SQS message
        """
        _logger.info("Parsing message %s", raw_message)

        for field_enum in MANDATORY_FIELDS:
            if field_enum.value not in raw_message:
                raise InvalidMessageError(f"Missing Field {field_enum.value}")

        raw_body: str = raw_message[MessageFields.BODY.value]
        body = json.loads(self.__deserialize(raw_body))
        if not body:
            raise InvalidMessageError("Empty message body.")

        message_attrs = self.parse_message_attrs(body)

        timestamp = body.get(MessageFields.TIMESTAMP.value)
        if not timestamp:
            raise InvalidMessageError("Missing or empty message timestamp.")

        message_id = raw_message[MessageFields.MESSAGE_ID.value]
        if not message_id:
            raise InvalidMessageError("Miessaing message id.")

        receipt_handle = raw_message[MessageFields.RECEIPT_HANDLE.value]
        if not receipt_handle:
            raise InvalidMessageError("Receipt Handle missing.")

        return SQSMessage(
            message_id=message_id,
            receipt_handle=receipt_handle,
            body=body,
            timestamp=timestamp,
            attributes=message_attrs)
