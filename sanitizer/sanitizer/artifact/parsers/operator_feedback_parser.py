"""Event parser module."""
from typing import Iterator, Optional

from pydantic import ValidationError

from base.aws.model import SQSMessage
from base.model.artifacts import (
    SOSOperatorArtifact,
    PeopleCountOperatorArtifact,
    CameraBlockedOperatorArtifact,
    RecorderType,
    OperatorArtifact)
from kink import inject
from sanitizer.artifact.parsers.iparser import IArtifactParser
from sanitizer.exceptions import ArtifactException
from sanitizer.message.models.operator_feedback_messages import (
    ParsedCameraBlockedOperatorMessage,
    ParsedPeopleCountOperatorMessage,
    ParsedSOSOperatorMessage,
    parse_operator_message,
    OperatorFeedbackMessage)


@inject
class OperatorFeedbackParser(IArtifactParser):  # pylint: disable=too-few-public-methods
    """Event parser class."""

    def parse(self, sqs_message: SQSMessage, recorder_type: Optional[RecorderType]) -> Iterator[OperatorArtifact]:
        try:
            event_message_body: OperatorFeedbackMessage = parse_operator_message(sqs_message.body["Message"])
        except (ValidationError, KeyError) as ex:
            raise ArtifactException("Unable to parse event message into pydantic model.") from ex

        if isinstance(event_message_body, ParsedCameraBlockedOperatorMessage):
            metadata = event_message_body.metadata
            information = event_message_body.additional_information
            yield CameraBlockedOperatorArtifact(tenant_id=metadata.tenant,
                                                device_id=metadata.device_id,
                                                event_timestamp=metadata.event_timestamp,
                                                operator_monitoring_start=metadata.operator_monitoring_start,
                                                operator_monitoring_end=metadata.operator_monitoring_end,
                                                additional_information=information.parse_additional_information(),
                                                is_chc_correct=event_message_body.camera_blocked.is_chc_correct)
        elif isinstance(event_message_body, ParsedPeopleCountOperatorMessage):
            metadata = event_message_body.metadata
            information = event_message_body.additional_information
            people_count = event_message_body.people_count
            yield PeopleCountOperatorArtifact(tenant_id=metadata.tenant,
                                              device_id=metadata.device_id,
                                              event_timestamp=metadata.event_timestamp,
                                              operator_monitoring_start=metadata.operator_monitoring_start,
                                              operator_monitoring_end=metadata.operator_monitoring_end,
                                              additional_information=information.parse_additional_information(),
                                              is_people_count_correct=people_count.is_people_count_correct,
                                              correct_count=people_count.correct_count)
        elif isinstance(event_message_body, ParsedSOSOperatorMessage):
            metadata = event_message_body.metadata
            information = event_message_body.additional_information
            yield SOSOperatorArtifact(tenant_id=metadata.tenant,
                                      device_id=metadata.device_id,
                                      event_timestamp=metadata.event_timestamp,
                                      operator_monitoring_start=metadata.operator_monitoring_start,
                                      operator_monitoring_end=metadata.operator_monitoring_end,
                                      additional_information=information.parse_additional_information(),
                                      reason=event_message_body.sos.reason)
        else:
            raise ArtifactException("Unable to determine event type from message.")
