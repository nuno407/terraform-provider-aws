# pylint: disable=no-self-argument, line-too-long
""" Artifact model. """
import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Literal, Optional, Union

from pydantic import Field, parse_obj_as, parse_raw_as, validator
from pydantic.dataclasses import dataclass
from pydantic.json import pydantic_encoder

from base.model.config import dataclass_config


class RecorderType(Enum):
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


@dataclass
class Resolution:
    """Resolution"""
    width: int
    height: int


@dataclass
class TimeWindow:
    """Defines a timezone aware time window in the past"""
    start: datetime
    end: datetime

    @validator("start", "end")
    def check_not_newer_than_now(cls, value: datetime) -> datetime:
        """Validate timestamp"""
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone aware")
        # check if timestamp is in the future
        if value > datetime.now(tz=value.tzinfo):
            raise ValueError("timestamp must be in the past")
        return value


@dataclass(config=dataclass_config)
class Artifact(ABC):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    s3_path: Optional[str] = Field(default=None)

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return json.dumps(self, default=pydantic_encoder)

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


@dataclass
class ImageBasedArtifact(Artifact):
    """Base class for image based artifacts"""
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


@dataclass
class VideoArtifact(ImageBasedArtifact):
    """Video artifact"""
    end_timestamp: datetime = Field(default=...)
    actual_duration: Optional[float] = Field(default=None)

    @validator("recorder")
    def check_recorder(cls, value: RecorderType) -> RecorderType:
        """Validate recorder type"""
        if value not in [RecorderType.INTERIOR, RecorderType.FRONT, RecorderType.TRAINING]:
            raise ValueError("Invalid recorder type")
        return value

    @validator("end_timestamp")
    def check_end_timestamp(cls, value: datetime) -> datetime:
        """Validate end timestamp"""
        return super().check_timestamp(value)

    @property
    def duration(self) -> float:
        """ duration of the video in seconds. """
        return (self.end_timestamp - self.timestamp).total_seconds()


@dataclass
class KinesisVideoArtifact(VideoArtifact):
    """Represents a video artifact that was recorded to Kinesis Video Streams"""
    stream_name: str = Field(default=...)

    actual_timestamp: Optional[datetime] = Field(default=None)
    actual_end_timestamp: Optional[datetime] = Field(default=None)

    @property
    def artifact_id(self) -> str:
        return f"{self.stream_name}_{round(self.timestamp.timestamp()*1000)}_{round(self.end_timestamp.timestamp()*1000)}"


@dataclass
class S3VideoArtifact(VideoArtifact):
    """Represents a video artifact that has been concatenated by RCC and uploaded to S3"""
    rcc_s3_path: str = Field(default=...)
    footage_id: str = Field(default=...)

    @property
    def artifact_id(self) -> str:
        return f"{self.device_id}_{self.recorder.value}_{self.footage_id}_{round(self.timestamp.timestamp()*1000)}_{round(self.end_timestamp.timestamp()*1000)}"

    @validator("rcc_s3_path")
    def check_rcc_s3_path(cls, value: str) -> str:
        """Validates that s3_path starts with s3://"""
        if not value.startswith("s3://"):
            raise ValueError("rcc_s3_path must start with s3://")
        return value


@dataclass
class SnapshotArtifact(ImageBasedArtifact):
    """Snapshot artifact"""
    uuid: str = Field(default=...)

    @validator("recorder")
    def check_recorder(cls, value: RecorderType) -> RecorderType:
        """Validate recorder type"""
        if value not in [RecorderType.SNAPSHOT, RecorderType.INTERIOR_PREVIEW]:
            raise ValueError("Invalid recorder type")
        return value

    @property
    def artifact_id(self) -> str:
        uuid_without_ext = self.uuid.rstrip(Path(self.uuid).suffix)
        return f"{self.tenant_id}_{self.device_id}_{uuid_without_ext}_{round(self.timestamp.timestamp()*1000)}"


@dataclass
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


@dataclass
class MetadataArtifact(Artifact):
    """ Metadata """
    referred_artifact: Union[SnapshotArtifact, KinesisVideoArtifact,
                             S3VideoArtifact, VideoArtifact, MultiSnapshotArtifact] = Field(default=...)
    metadata_type: Literal[MetadataType.IMU, MetadataType.SIGNALS, MetadataType.PREVIEW] = Field(default=...)

    @property
    def artifact_id(self) -> str:
        """Artifact ID for metadata artifacts"""
        return f"{self.referred_artifact.artifact_id}_{self.metadata_type.value}"


@dataclass
class IMUArtifact(MetadataArtifact):
    """ IMU Artifact """
    metadata_type: Literal[MetadataType.IMU] = MetadataType.IMU


@dataclass
class SignalsArtifact(MetadataArtifact):
    """ Signals Artifact """
    metadata_type: Literal[MetadataType.SIGNALS] = MetadataType.SIGNALS


@dataclass
class PreviewSignalsArtifact(MetadataArtifact):
    """ Preview Signals Artifact, that contains compacted signals data """
    timestamp: datetime = Field(default=...)
    end_timestamp: datetime = Field(default=...)
    referred_artifact: MultiSnapshotArtifact = Field(default=...)
    metadata_type: Literal[MetadataType.PREVIEW] = MetadataType.PREVIEW


def parse_artifact(json_data: Union[str, dict]) -> Artifact:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return parse_obj_as(Union[KinesisVideoArtifact,  # type: ignore
                                  S3VideoArtifact,
                                  SnapshotArtifact,
                                  SignalsArtifact,
                                  IMUArtifact,
                                  MultiSnapshotArtifact,
                                  PreviewSignalsArtifact],
                            json_data)  # type: ignore
    return parse_raw_as(Union[KinesisVideoArtifact,  # type: ignore
                              S3VideoArtifact,
                              SnapshotArtifact,
                              SignalsArtifact,
                              IMUArtifact,
                              MultiSnapshotArtifact,
                              PreviewSignalsArtifact],
                        json_data)  # type: ignore
