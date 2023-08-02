from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, Extra, Field

from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType,
                                    Location, Shutdown)


class ConfiguredBaseModel(BaseModel):
    class Config:
        extra = Extra.ignore
        use_enum_values = True
        validate_assignment = True
        allow_population_by_field_name = True


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


class IncidentEventContent(BaseEventContent):
    incident_type: IncidentType = Field()
    location: Location = Field()
    bundle_id: str = Field()


class CameraServiceEventContent(BaseEventContent):
    camera_name: str = Field()
    service_status: GeneralServiceState = Field()
    camera_service_description: list[CameraServiceState] = Field(default_factory=list)


class DeviceInfoEventContent(BaseEventContent):
    system_report: Optional[str] = Field(default=None)
    software_versions: list[dict[str, Any]] = Field(default_factory=list)
    device_type: str = Field()
    last_shutdown: Optional[Shutdown] = Field(alias="last_shutdown_reason")


class MessageBodyMessageValue(ConfiguredBaseModel):
    properties: Union[IncidentEventContent, CameraServiceEventContent, DeviceInfoEventContent] = Field()


class MessageBodyMessage(ConfiguredBaseModel):
    topic: str = Field()
    headers: dict = Field(default_factory=dict)
    path: str = Field()
    value: MessageBodyMessageValue = Field()
    extra: dict = Field(default_factory=dict)
    timestamp: datetime = Field()


class MessageBodyAttributes(ConfiguredBaseModel):
    device_type: StrOrWrappedString = Field(alias="deviceType")
    modified_thing_category: StrOrWrappedString = Field(alias="modifiedThingCategory")
    operation_mode: StrOrWrappedString = Field(alias="operationMode")
    event_type: StrOrWrappedString = Field(alias="eventType")
    tenant_id: StrOrWrappedString = Field(alias="tenant")
    subject_id: StrOrWrappedString = Field(alias="subjectId")


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
