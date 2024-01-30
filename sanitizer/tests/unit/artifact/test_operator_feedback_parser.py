import pytest

from base.aws.model import SQSMessage
from base.model.artifacts import (SOSOperatorArtifact, PeopleCountOperatorArtifact, OperatorArtifact,
                                  CameraBlockedOperatorArtifact, OperatorAdditionalInformation, OperatorSOSReason)
from sanitizer.artifact.parsers.operator_feedback_parser import OperatorFeedbackParser
from sanitizer.exceptions import ArtifactException
from helper_functions import parse_sqs_message


def get_additional_info() -> OperatorAdditionalInformation:
    return OperatorAdditionalInformation(
        is_door_blocked=True,
        is_camera_blocked=False,
        is_audio_malfunction=True,
        observations="Garfield the cat blocked the door"
    )


def load_sos_message(reason: str) -> SQSMessage:
    msg = parse_sqs_message("valid_sos_sav.json")
    msg.body["Message"]["sos"]["reason"] = reason
    return msg


@pytest.mark.unit
@pytest.mark.parametrize("input_message,expected", [
    (
        parse_sqs_message("valid_camerablock_sav.json"),
        [
            CameraBlockedOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                is_chc_correct=False
            )
        ]
    ),
    (
        parse_sqs_message("valid_people_count_sav.json"),
        [
            PeopleCountOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                is_people_count_correct=False,
                correct_count=5
            )
        ]
    ),
    (
        load_sos_message("ACCIDENTAL"),
        [
            SOSOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                reason=OperatorSOSReason.ACCIDENTAL
            )
        ]
    ),
    (
        load_sos_message("TECHNICAL_ISSUE"),
        [
            SOSOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                reason=OperatorSOSReason.TECHNICAL_ISSUE
            )
        ]
    ),
    (
        load_sos_message("RIDE_INTERRUPTION"),
        [
            SOSOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                reason=OperatorSOSReason.RIDE_INTERRUPTION
            )
        ]
    ),
    (
        load_sos_message("HEALTH_ISSUE"),
        [
            SOSOperatorArtifact(
                tenant_id="datanauts",
                device_id="DATANAUTS_DEV_02",
                event_timestamp="2023-08-29T08:17:15+00:00",
                operator_monitoring_start="2023-08-29T08:18:49+00:00",
                operator_monitoring_end="2023-08-29T08:35:57+00:00",
                additional_information=get_additional_info(),
                reason=OperatorSOSReason.HEALTH_ISSUE
            )
        ]
    )
])
def test_operator_feedback_parser(input_message: SQSMessage,
                                  expected: list[OperatorArtifact]):
    got_events = OperatorFeedbackParser().parse(input_message, None)
    assert list(got_events) == expected


@pytest.mark.unit
@pytest.mark.parametrize("input_message,expected_exception", [
    (
        load_sos_message("WRONG_REASON"),
        ArtifactException
    ),
    (
        parse_sqs_message("valid_camera_service_event.json"),
        ArtifactException
    )
])
def test_operator_feedback_parser_fails_as_expected(input_message: SQSMessage,
                                                    expected_exception: type):
    with pytest.raises(expected_exception):
        list(OperatorFeedbackParser().parse(input_message, None))
