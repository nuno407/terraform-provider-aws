""" aws models module. """
import json
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel
from datetime import datetime


class S3ObjectInfo(BaseModel):
    """Info about an S3 object used to retrun information about list_objects_v2"""
    key: str
    date_modified: datetime
    size: int

    def get_file_name(self) -> str:
        """Parse filename from the key"""
        return self.key.split("/")[-1]

    class Config:
        frozen = True


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
