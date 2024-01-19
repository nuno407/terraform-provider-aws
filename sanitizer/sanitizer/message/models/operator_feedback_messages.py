from pydantic import Field, TypeAdapter
from datetime import datetime
from typing import Optional, Union, NewType
from base.model.artifacts import OperatorSOSReason, OperatorAdditionalInformation
from base.model.base_model import ConfiguredBaseModel


class AdditonalInformation(ConfiguredBaseModel):
    is_door_blocked: bool = Field(alias="isDoorBlocked")
    is_camera_blocked: bool = Field(alias="isCameraBlocked")
    is_audio_malfunction: bool = Field(alias="isAudioMalfunction")
    observations: str = Field(default="observations")

    def parse_additional_information(self) -> OperatorAdditionalInformation:
        return OperatorAdditionalInformation(is_door_blocked=self.is_door_blocked,
                                             is_camera_blocked=self.is_camera_blocked,
                                             is_audio_malfunction=self.is_audio_malfunction,
                                             observations=self.observations)


class Metadata(ConfiguredBaseModel):
    device_id: str = Field(alias="deviceId")
    tenant: str = Field(alias="tenant")
    event_timestamp: datetime = Field(alias="eventTimestamp")
    operator_monitoring_start: datetime = Field(alias="operatorMonitoringStart")
    operator_monitoring_end: datetime = Field(alias="operatorMonitoringEnd")


class SOS(ConfiguredBaseModel):
    reason: OperatorSOSReason = Field(default=...)


class CameraBlockedOperator(ConfiguredBaseModel):
    is_chc_correct: bool = Field(alias="isChcCorrect")


class PeopleCountOperator(ConfiguredBaseModel):
    is_people_count_correct: bool = Field(alias="isPeopleCountCorrect")
    correct_count: Optional[int] = Field(alias="correctCount", default=None)


class ParsedCameraBlockedOperatorMessage(ConfiguredBaseModel):
    metadata: Metadata = Field(alias="metadata")
    additional_information: AdditonalInformation = Field(alias="additionalInformation")
    camera_blocked: CameraBlockedOperator = Field(alias="cameraBlocked")


class ParsedPeopleCountOperatorMessage(ConfiguredBaseModel):
    metadata: Metadata = Field(alias="metadata")
    additional_information: AdditonalInformation = Field(alias="additionalInformation")
    people_count: PeopleCountOperator = Field(alias="peopleCount")


class ParsedSOSOperatorMessage(ConfiguredBaseModel):
    metadata: Metadata = Field(alias="metadata")
    additional_information: AdditonalInformation = Field(alias="additionalInformation")
    sos: SOS = Field(alias="sos")


OperatorFeedbackMessage = Union[ParsedCameraBlockedOperatorMessage,
                                ParsedPeopleCountOperatorMessage,
                                ParsedSOSOperatorMessage]

OperatorFeedbackMessageAdapter = TypeAdapter(OperatorFeedbackMessage)


def parse_operator_message(sqs_message: dict[str, str]) -> OperatorFeedbackMessage:  # type: ignore
    return OperatorFeedbackMessageAdapter.validate_python(sqs_message)  # type: ignore
