from datetime import datetime
from typing import Any, Optional, Union

from pydantic import field_validator, Field

from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType, Shutdown)
from base.model.base_model import ConfiguredBaseModel


class WrappedString(ConfiguredBaseModel):
    value: str = Field(alias="Value")

    def __str__(self) -> str:
        return self.value


class WrappedStringValue(ConfiguredBaseModel):
    value: str = Field(alias="StringValue")

    def __str__(self) -> str:
        return self.value


StrOrWrappedString = Union[str, WrappedString, WrappedStringValue]


class EventHeader(ConfiguredBaseModel):
    message_type: EventType = Field()
    timestamp: datetime = Field(alias="timestamp_ms")
    device_id: str = Field()


class BaseEventContent(ConfiguredBaseModel):
    header: EventHeader = Field()

    @classmethod
    def _check_event_type(cls, header: EventHeader, expected_event_type: EventType):
        if not isinstance(header, EventHeader):
            raise ValueError("Header must be of type EventHeader")
        if not header.message_type == expected_event_type:
            raise ValueError("Event type in header does not match the parsed event.")


class IncidentEventContent(BaseEventContent):
    incident_type: Optional[IncidentType] = Field(default=None)
    bundle_id: Optional[str] = Field(default=None)

    @field_validator("header")
    @classmethod
    def _validate_header(cls, value: EventHeader):
        BaseEventContent._check_event_type(value, EventType.INCIDENT)
        return value


class CameraServiceEventContent(BaseEventContent):
    camera_name: str = Field()
    service_status: Optional[GeneralServiceState] = Field(default=None)
    camera_service_description: list[CameraServiceState] = Field(default_factory=list)

    @field_validator("header")
    @classmethod
    def _validate_header(cls, value: EventHeader):
        BaseEventContent._check_event_type(value, EventType.CAMERA_SERVICE)
        return value


class DeviceInfoEventContent(BaseEventContent):
    system_report: Optional[str] = Field(default=None)
    software_versions: list[dict[str, Any]] = Field(default_factory=list)
    device_type: Optional[str] = Field(default=None)
    last_shutdown: Optional[Shutdown] = Field(alias="last_shutdown_reason", default=None)

    @field_validator("header")
    @classmethod
    def _validate_header(cls, value: EventHeader):
        BaseEventContent._check_event_type(value, EventType.DEVICE_INFO)
        return value


class MessageBodyMessageValue(ConfiguredBaseModel):
    properties: Union[IncidentEventContent, CameraServiceEventContent,
                      DeviceInfoEventContent] = Field()


class MessageBodyMessage(ConfiguredBaseModel):
    topic: str = Field()
    headers: dict = Field(default_factory=dict)
    path: str = Field()
    value: MessageBodyMessageValue = Field()
    extra: dict = Field(default_factory=dict)
    timestamp: datetime = Field()


class MessageBodyAttributes(ConfiguredBaseModel):
    tenant_id: StrOrWrappedString = Field(alias="tenant")


class MessageBody(ConfiguredBaseModel):
    topic_arn: str = Field(alias="TopicArn")
    message: MessageBodyMessage = Field(alias="Message")
    message_attributes: MessageBodyAttributes = Field(alias="MessageAttributes")


class MessageAttributes(ConfiguredBaseModel):
    sent_timestamp: Optional[datetime] = Field(alias="SentTimestamp")
    approximate_receive_count: int = Field(alias="ApproximateReceiveCount")


class EventMessage(ConfiguredBaseModel):
    message_id: str = Field(alias="MessageId")
    receipt_handle: str = Field(alias="ReceiptHandle")
    body: MessageBody = Field(alias="Body")
    attributes: MessageAttributes = Field(alias="Attributes")
