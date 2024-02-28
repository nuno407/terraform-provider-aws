# pylint: disable=no-self-argument, line-too-long, no-member
""" Artifact model. """
import hashlib
from abc import abstractmethod
from enum import Enum
from typing import Literal, Optional, Union, Annotated, TYPE_CHECKING
from datetime import datetime
from pydantic import field_validator, Field, TypeAdapter
from base.model.validators import UtcDatetimeInPast
from base.model.base_model import ConfiguredBaseModel, S3Path, AnonymizedS3Path, RawS3Path
from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType, Shutdown)


class RecorderType(str, Enum):
    """ artifact type enumerator. """
    FRONT = "FrontRecorder"
    INTERIOR = "InteriorRecorder"
    INTERIOR_PREVIEW = "InteriorRecorderPreview"
    TRAINING = "TrainingRecorder"
    SNAPSHOT = "TrainingMultiSnapshot"


class MetadataType(str, Enum):
    """ metadata type enumerator """
    SIGNALS = "metadata_full"
    PREVIEW = "metadata_preview"
    IMU = "IMU"


class OperatorSOSReason(str, Enum):
    """ reason for operator sos button """
    ACCIDENTAL = "ACCIDENTAL"
    TECHNICAL_ISSUE = "TECHNICAL_ISSUE"
    RIDE_INTERRUPTION = "RIDE_INTERRUPTION"
    HEALTH_ISSUE = "HEALTH_ISSUE"
    OTHER = "OTHER"


class Resolution(ConfiguredBaseModel):
    """Resolution"""
    width: int
    height: int


class TimeWindow(ConfiguredBaseModel):
    """Defines a timezone aware time window in the past"""
    start: datetime = Field(default=...)
    end: datetime = Field(default=...)


class Artifact(ConfiguredBaseModel):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    s3_path: Optional[S3Path] = Field(default=None)

    if TYPE_CHECKING:
        @property
        @abstractmethod
        def artifact_id(self) -> str:
            """Artifact ID."""

    @property
    def devcloudid(self) -> str:
        """Compute hash for checking data completion"""
        return hashlib.sha256(self.artifact_id.encode("utf-8")).hexdigest()


class ImageBasedArtifact(Artifact):
    """Base class for image based artifacts"""
    artifact_id: str = Field()
    resolution: Optional[Resolution] = Field(default=None)
    recorder: RecorderType = Field(default=...)
    timestamp: UtcDatetimeInPast = Field(default=...)
    end_timestamp: UtcDatetimeInPast = Field(default=...)
    upload_timing: TimeWindow = Field(default=...)


class VideoArtifact(ImageBasedArtifact):
    """Video artifact"""
    end_timestamp: UtcDatetimeInPast = Field(default=...)
    actual_duration: Optional[float] = Field(default=None)
    recorder: Literal[RecorderType.INTERIOR, RecorderType.FRONT,
                      RecorderType.TRAINING] = Field(default=...)

    @property
    def duration(self) -> float:
        """ duration of the video in seconds. """
        return (self.end_timestamp - self.timestamp).total_seconds()


class Recording(ConfiguredBaseModel):
    """Represents a recording represented by a recording id and the corresponding chunk ids"""
    recording_id: str = Field(default=...)
    chunk_ids: list[int] = Field(default=...)


class S3VideoArtifact(VideoArtifact):
    """Represents a video artifact that has been concatenated by RCC and uploaded to S3"""
    artifact_name: Literal["s3_video"] = "s3_video"
    rcc_s3_path: S3Path
    footage_id: str = Field(default=...)
    recordings: list[Recording] = Field(default=...)
    raw_s3_path: RawS3Path = Field()
    anonymized_s3_path: AnonymizedS3Path = Field()


class SnapshotArtifact(ImageBasedArtifact):
    """Snapshot artifact"""
    artifact_name: Literal["snapshot"] = "snapshot"
    uuid: str = Field(default=...)
    recorder: Literal[RecorderType.SNAPSHOT, RecorderType.INTERIOR_PREVIEW] = Field(default=...)
    raw_s3_path: RawS3Path = Field()
    anonymized_s3_path: AnonymizedS3Path = Field()


class MultiSnapshotArtifact(ImageBasedArtifact):
    """An artifact that contains multiple snapshots"""
    artifact_name: Literal["multi_snapshot"] = "multi_snapshot"
    chunks: list[SnapshotArtifact] = Field(default=...)
    recording_id: str = Field(default=...)

    @field_validator("chunks")
    @classmethod
    def check_sorted_chunks(cls, chunks: list[SnapshotArtifact]) -> list[SnapshotArtifact]:
        """Ensure snapshot chunks are sorted"""
        chunks.sort(key=lambda x: x.timestamp)
        return chunks


class MetadataArtifact(Artifact):
    """ Metadata """
    referred_artifact: Union[SnapshotArtifact,
                             S3VideoArtifact, VideoArtifact, MultiSnapshotArtifact] = Field(default=...)
    metadata_type: Literal[MetadataType.IMU, MetadataType.SIGNALS,
                           MetadataType.PREVIEW] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        """Artifact ID for metadata artifacts"""
        return f"{self.referred_artifact.artifact_id}_{self.metadata_type.value}"


class IMUArtifact(MetadataArtifact):
    """ IMU Artifact """
    artifact_name: Literal["raw_imu"] = "raw_imu"
    metadata_type: Literal[MetadataType.IMU] = MetadataType.IMU


class SignalsArtifact(MetadataArtifact):
    """ Signals Artifact """
    artifact_name: Literal["raw_signals"] = "raw_signals"
    metadata_type: Literal[MetadataType.SIGNALS] = MetadataType.SIGNALS


class PreviewSignalsArtifact(MetadataArtifact):
    """ Preview Signals Artifact, that contains compacted signals data """
    artifact_name: Literal["raw_preview_signals"] = "raw_preview_signals"
    timestamp: UtcDatetimeInPast = Field(default=...)
    end_timestamp: UtcDatetimeInPast = Field(default=...)
    referred_artifact: MultiSnapshotArtifact = Field(default=...)
    metadata_type: Literal[MetadataType.PREVIEW] = MetadataType.PREVIEW


class EventArtifact(Artifact):
    """Base class for all event artifacts"""
    timestamp: UtcDatetimeInPast = Field(default=...)
    event_name: Literal[EventType.INCIDENT, EventType.DEVICE_INFO,
                        EventType.CAMERA_SERVICE] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        """Artifact ID for event artifacts"""
        event_name = self.event_name.value.split(".")[-1]
        return f"{self.tenant_id}_{self.device_id}_{event_name}_{round(self.timestamp.timestamp() * 1000)}"


class IncidentEventArtifact(EventArtifact):
    """Represents an incident event from RCC"""
    artifact_name: Literal["incident_info"] = "incident_info"
    event_name: Literal[EventType.INCIDENT] = EventType.INCIDENT
    incident_type: IncidentType = Field(default=IncidentType.UNKNOWN)
    bundle_id: Optional[str] = Field(default=None)


class DeviceInfoEventArtifact(EventArtifact):
    """Represents a device info event from RCC"""
    artifact_name: Literal["device_info"] = "device_info"
    event_name: Literal[EventType.DEVICE_INFO] = EventType.DEVICE_INFO
    system_report: Optional[str] = Field(default=None)
    software_versions: list[dict[str, str]] = Field(default_factory=list)
    device_type: Optional[str] = Field(default=None)
    last_shutdown: Optional[Shutdown] = Field(default=None)


class CameraServiceEventArtifact(EventArtifact):
    """Represents a camera service event from RCC"""
    artifact_name: Literal["camera_service"] = "camera_service"
    event_name: Literal[EventType.CAMERA_SERVICE] = EventType.CAMERA_SERVICE
    service_state: GeneralServiceState = Field(default=GeneralServiceState.UNKNOWN)
    camera_name: Optional[str] = Field(default=None)
    camera_state: list[CameraServiceState] = Field(default_factory=list)


class OperatorAdditionalInformation(ConfiguredBaseModel):
    """Represents the checkbox form data colleced in the SAV Operator UI"""
    is_door_blocked: bool = Field(default=...)
    is_camera_blocked: bool = Field(default=...)
    is_audio_malfunction: bool = Field(default=...)
    observations: Optional[str] = Field(default=None)


class OperatorArtifact(Artifact):
    """Represents the footage seen by an SAV Operator as part of an incident"""
    artifact_name: str = "sav-operator"
    event_timestamp: UtcDatetimeInPast = Field(default=...)
    operator_monitoring_start: UtcDatetimeInPast = Field(default=...)
    operator_monitoring_end: UtcDatetimeInPast = Field(default=...)

    @property
    def artifact_id(self) -> str:
        return f"{self.artifact_name}_{self.tenant_id}_{self.device_id}_{round(self.event_timestamp.timestamp() *1000)}"


class SOSOperatorArtifact(OperatorArtifact):
    """Represents an SAV SOS event"""
    artifact_name: Literal["sav-operator-sos"] = "sav-operator-sos"
    additional_information: OperatorAdditionalInformation = Field(default=...)
    reason: OperatorSOSReason = Field(default=...)


class PeopleCountOperatorArtifact(OperatorArtifact):
    """Represents people count predicted by ivs_fc model vs observed by SAV Operator"""
    artifact_name: Literal["sav-operator-people-count"] = "sav-operator-people-count"
    additional_information: OperatorAdditionalInformation = Field(default=...)
    is_people_count_correct: bool = Field(default=...)
    correct_count: Optional[int] = Field(default=None)


class CameraBlockedOperatorArtifact(OperatorArtifact):
    """Represents a camera service event from RCC"""
    artifact_name: Literal["sav-operator-camera-blocked"] = "sav-operator-camera-blocked"
    additional_information: OperatorAdditionalInformation = Field(default=...)
    is_chc_correct: bool = Field(default=...)


class OtherOperatorArtifact(OperatorArtifact):
    """Represents an SAV SOS event"""
    artifact_name: Literal["sav-operator-other"] = "sav-operator-other"
    additional_information: OperatorAdditionalInformation = Field(default=...)
    field_type: str = Field(default=..., alias="type")


RCCArtifacts = Union[S3VideoArtifact,  # type: ignore # pylint: disable=invalid-name
                     SnapshotArtifact,
                     SignalsArtifact,
                     IMUArtifact,
                     MultiSnapshotArtifact,
                     PreviewSignalsArtifact,
                     IncidentEventArtifact,
                     CameraServiceEventArtifact,
                     DeviceInfoEventArtifact,
                     CameraBlockedOperatorArtifact,
                     PeopleCountOperatorArtifact,
                     SOSOperatorArtifact,
                     OtherOperatorArtifact]

DiscriminatedRCCArtifactsTypeAdapter = TypeAdapter(Annotated[RCCArtifacts,
                                                             Field(..., discriminator="artifact_name")])


def parse_artifact(json_data: Union[str, dict]) -> Artifact:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return DiscriminatedRCCArtifactsTypeAdapter.validate_python(json_data)  # type: ignore
    return DiscriminatedRCCArtifactsTypeAdapter.validate_json(json_data)  # type: ignore
