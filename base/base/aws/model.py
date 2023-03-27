""" aws models module. """
import json
from dataclasses import dataclass
from typing import Optional

@dataclass
class MessageAttributes:
    """Message attributes."""
    tenant: Optional[str]
    device_id: Optional[str] = None


@dataclass
class SQSMessage:
    """SQS Message dataclass."""
    message_id: str
    receipt_handle: str
    timestamp: str
    body: dict
    attributes: MessageAttributes

    def stringify(self) -> str:
        """returns string JSON representation version of message

        Returns:
            str: JSON representation
        """
        return json.dumps(self, default=lambda o: o.__dict__)
