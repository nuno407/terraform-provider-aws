"""Event parser module."""
from typing import Iterator, Optional

from pydantic import ValidationError

from base.aws.model import SQSMessage
from base.model.artifacts import (CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact, EventArtifact,
                                  IncidentEventArtifact, RecorderType)
from base.model.event_types import EventType
from kink import inject
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.exceptions import InvalidMessageError
from sanitizer.message.models.event_messages import (CameraServiceEventContent,
                                                     DeviceInfoEventContent,
                                                     IncidentEventContent)
from sanitizer.message.models.event_messages import \
    MessageBody as EventMessageBody


@inject
class EventParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """Event parser class."""

    def parse(self, sqs_message: SQSMessage, recorder_type: Optional[RecorderType]) -> Iterator[EventArtifact]:
        try:
            event_message_body = EventMessageBody.model_validate(sqs_message.body)
        except ValidationError as ex:
            raise InvalidMessageError("Unable to parse event message into pydantic model.") from ex

        event_name: EventType = event_message_body.message.value.properties.header.message_type
        tenant_id = str(event_message_body.message_attributes.tenant_id)
        device_id = event_message_body.message.value.properties.header.device_id
        timestamp = event_message_body.message.value.properties.header.timestamp
        event_object = event_message_body.message.value.properties

        try:
            if event_name == EventType.DEVICE_INFO and isinstance(event_object, DeviceInfoEventContent):
                yield DeviceInfoEventArtifact(tenant_id=tenant_id,
                                              device_id=device_id,
                                              timestamp=timestamp,
                                              event_name=event_name,
                                              system_report=event_object.system_report,
                                              software_versions=event_object.software_versions,
                                              device_type=event_object.device_type,
                                              last_shutdown=event_object.last_shutdown)
            elif event_name == EventType.CAMERA_SERVICE and isinstance(event_object, CameraServiceEventContent):
                yield CameraServiceEventArtifact(tenant_id=tenant_id,
                                                 device_id=device_id,
                                                 timestamp=timestamp,
                                                 event_name=event_name,
                                                 camera_name=event_object.camera_name,
                                                 service_state=event_object.service_status,
                                                 camera_state=event_object.camera_service_description)
            elif event_name == EventType.INCIDENT and isinstance(event_object, IncidentEventContent):
                yield IncidentEventArtifact(tenant_id=tenant_id,
                                            device_id=device_id,
                                            timestamp=timestamp,
                                            event_name=event_name,
                                            incident_type=event_object.incident_type,
                                            bundle_id=event_object.bundle_id)
            else:
                raise InvalidMessageError("Unable to determine event type from message.")
        except ValidationError as ex:
            raise InvalidMessageError("Unable to parse event message into pydantic model.") from ex
