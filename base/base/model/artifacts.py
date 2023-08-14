# pylint: disable=no-self-argument, line-too-long
""" Artifact model. """
import hashlib
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Literal, Optional, Union

from pydantic import Field, parse_obj_as, parse_raw_as, validator

from base.model.config import ConfiguredBaseModel
from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType,
                                    Location, Shutdown)


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


class Resolution(ConfiguredBaseModel):
    """Resolution"""
    width: int
    height: int


class TimeWindow(ConfiguredBaseModel):
    """Defines a timezone aware time window in the past"""
    start: datetime = Field(default=...)
    end: datetime = Field(default=...)

    @validator("start", "end")
    def check_not_newer_than_now(cls, value: datetime) -> datetime:
        """Validate timestamp"""
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone aware")
        # check if timestamp is in the future
        if value > datetime.now(tz=value.tzinfo):
            raise ValueError("timestamp must be in the past")
        return value


class Artifact(ConfiguredBaseModel):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    s3_path: Optional[str] = Field(default=None)

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return self.json(by_alias=True, exclude_unset=False, exclude_none=True)

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Artifact ID."""

    @validator("s3_path")
    def check_s3_path(cls, value: Optional[str]) -> Optional[str]:
        """Validates that s3_path starts with s3://"""
        if value and not value.startswith("s3://"):
            raise ValueError("s3_path must start with s3://")
        return value

    @property
    def devcloudid(self) -> str:
        """Compute hash for checking data completion"""
        return hashlib.sha256(self.artifact_id.encode("utf-8")).hexdigest()


class ImageBasedArtifact(Artifact):
    """Base class for image based artifacts"""
    # pylint: disable=abstract-method
    resolution: Optional[Resolution] = Field(default=None)
    recorder: RecorderType = Field(default=...)
    timestamp: datetime = Field(default=...)
    end_timestamp: datetime = Field(default=...)
    upload_timing: TimeWindow = Field(default=...)

    @validator("timestamp")
    def check_timestamp(cls, value: datetime) -> datetime:
        """Validate timestamp"""
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone aware")
        # check if timestamp is in the future
        if value > datetime.now(tz=value.tzinfo):
            raise ValueError("timestamp must be in the past")
        return value


class VideoArtifact(ImageBasedArtifact):
    """Video artifact"""
    # pylint: disable=abstract-method
    end_timestamp: datetime = Field(default=...)
    actual_duration: Optional[float] = Field(default=None)
    recorder: Literal[RecorderType.INTERIOR, RecorderType.FRONT, RecorderType.TRAINING] = Field(default=...)

    @validator("end_timestamp")
    def check_end_timestamp(cls, value: datetime) -> datetime:
        """Validate end timestamp"""
        return super().check_timestamp(value)

    @property
    def duration(self) -> float:
        """ duration of the video in seconds. """
        return (self.end_timestamp - self.timestamp).total_seconds()


class KinesisVideoArtifact(VideoArtifact):
    """Represents a video artifact that was recorded to Kinesis Video Streams"""
    stream_name: str = Field(default=...)

    actual_timestamp: Optional[datetime] = Field(default=None)
    actual_end_timestamp: Optional[datetime] = Field(default=None)

    @property
    def artifact_id(self) -> str:
        return f"{self.stream_name}_{round(self.timestamp.timestamp()*1000)}_{round(self.end_timestamp.timestamp()*1000)}"


class Recording(ConfiguredBaseModel):
    """Represents a recording represented by a recording id and the corresponding chunk ids"""
    recording_id: str = Field(default=...)
    chunk_ids: list[int] = Field(default=...)


class S3VideoArtifact(VideoArtifact):
    """Represents a video artifact that has been concatenated by RCC and uploaded to S3"""
    rcc_s3_path: str = Field(default=...)
    footage_id: str = Field(default=...)
    recordings: list[Recording] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        return f"{self.device_id}_{self.recorder.value}_{self.footage_id}_{round(self.timestamp.timestamp()*1000)}_{round(self.end_timestamp.timestamp()*1000)}"

    @validator("rcc_s3_path")
    def check_rcc_s3_path(cls, value: str) -> str:
        """Validates that s3_path starts with s3://"""
        if not value.startswith("s3://"):
            raise ValueError("rcc_s3_path must start with s3://")
        return value


class SnapshotArtifact(ImageBasedArtifact):
    """Snapshot artifact"""
    uuid: str = Field(default=...)
    recorder: Literal[RecorderType.SNAPSHOT, RecorderType.INTERIOR_PREVIEW] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        uuid_without_ext = self.uuid.removesuffix(Path(self.uuid).suffix)
        return f"{self.tenant_id}_{self.device_id}_{uuid_without_ext}_{round(self.timestamp.timestamp()*1000)}"


class MultiSnapshotArtifact(ImageBasedArtifact):
    """An artifact that continas multiple snapshots"""
    chunks: list[SnapshotArtifact] = Field(default=...)
    recording_id: str = Field(default=...)

    @validator("chunks")
    def check_sorted_chunks(cls, chunks: list[SnapshotArtifact]) -> list[SnapshotArtifact]:
        """Ensure snapshot chunks are sorted"""
        chunks.sort(key=lambda x: x.timestamp)
        return chunks

    @property
    def artifact_id(self) -> str:
        """Artifact ID for the multiple snapshots artifacts"""
        return f"{self.tenant_id}_{self.device_id}_{self.recording_id}_{round(self.timestamp.timestamp()*1000)}"


class MetadataArtifact(Artifact):
    """ Metadata """
    referred_artifact: Union[SnapshotArtifact, KinesisVideoArtifact,
                             S3VideoArtifact, VideoArtifact, MultiSnapshotArtifact] = Field(default=...)
    metadata_type: Literal[MetadataType.IMU, MetadataType.SIGNALS, MetadataType.PREVIEW] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        """Artifact ID for metadata artifacts"""
        return f"{self.referred_artifact.artifact_id}_{self.metadata_type.value}"


class IMUArtifact(MetadataArtifact):
    """ IMU Artifact """
    metadata_type: Literal[MetadataType.IMU] = MetadataType.IMU


class SignalsArtifact(MetadataArtifact):
    """ Signals Artifact """
    metadata_type: Literal[MetadataType.SIGNALS] = MetadataType.SIGNALS


class PreviewSignalsArtifact(MetadataArtifact):
    """ Preview Signals Artifact, that contains compacted signals data """
    timestamp: datetime = Field(default=...)
    end_timestamp: datetime = Field(default=...)
    referred_artifact: MultiSnapshotArtifact = Field(default=...)
    metadata_type: Literal[MetadataType.PREVIEW] = MetadataType.PREVIEW


class EventArtifact(Artifact):
    """Base class for all event artifacts"""
    timestamp: datetime = Field(default=...)
    event_name: Literal[EventType.INCIDENT, EventType.DEVICE_INFO, EventType.CAMERA_SERVICE] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        """Artifact ID for event artifacts"""
        event_name = self.event_name.value.split(".")[-1]
        return f"{self.tenant_id}_{self.device_id}_{event_name}_{round(self.timestamp.timestamp()*1000)}"


class IncidentEventArtifact(EventArtifact):
    """Represents an incident event from RCC"""
    event_name: Literal[EventType.INCIDENT] = Field(default=...)
    location: Optional[Location] = Field(default=None)
    incident_type: IncidentType = Field(default=IncidentType.UNKNOWN)
    bundle_id: Optional[str] = Field(default=None)


class DeviceInfoEventArtifact(EventArtifact):
    """Represents a device info event from RCC"""
    event_name: Literal[EventType.DEVICE_INFO] = Field(default=...)
    system_report: Optional[str] = Field(default=None)
    software_versions: list[dict[str, str]] = Field(default_factory=list)
    device_type: Optional[str] = Field(default=None)
    last_shutdown: Optional[Shutdown] = Field(default=None)


class CameraServiceEventArtifact(EventArtifact):
    """Represents a camera service event from RCC"""
    event_name: Literal[EventType.CAMERA_SERVICE] = Field(default=...)
    service_state: GeneralServiceState = Field(default=GeneralServiceState.UNKNOWN)
    camera_name: Optional[str] = Field(default=None)
    camera_state: list[CameraServiceState] = Field(default_factory=list)


def parse_artifact(json_data: Union[str, dict]) -> Artifact:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return parse_obj_as(Union[KinesisVideoArtifact,  # type: ignore
                                  S3VideoArtifact,
                                  SnapshotArtifact,
                                  SignalsArtifact,
                                  IMUArtifact,
                                  MultiSnapshotArtifact,
                                  PreviewSignalsArtifact,
                                  IncidentEventArtifact,
                                  CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact],
                            json_data)  # type: ignore
    return parse_raw_as(Union[KinesisVideoArtifact,  # type: ignore
                              S3VideoArtifact,
                              SnapshotArtifact,
                              SignalsArtifact,
                              IMUArtifact,
                              MultiSnapshotArtifact,
                              PreviewSignalsArtifact,
                              IncidentEventArtifact,
                              CameraServiceEventArtifact,
                              DeviceInfoEventArtifact],
                        json_data)  # type: ignore
