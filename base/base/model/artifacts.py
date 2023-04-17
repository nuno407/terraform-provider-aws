# pylint: disable=no-self-argument
""" Artifact model. """
import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Union

from pydantic import parse_raw_as, validator
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


@dataclass(config=dataclass_config)
class Artifact(ABC):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    recorder: RecorderType
    timestamp: datetime

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return json.dumps(self, default=pydantic_encoder)

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Artifact ID."""

    @validator("timestamp")
    def check_timestamp(cls, value: datetime) -> datetime:
        """Validate timestamp"""
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone aware")
        # check if timestamp is in the future
        if value > datetime.now(tz=value.tzinfo):
            raise ValueError("timestamp must be in the past")
        return value

    @property
    def devcloudid(self) -> str:
        """Compute hash for checking data completion"""
        return hashlib.sha256(self.artifact_id.encode("utf-8")).hexdigest()


@dataclass
class VideoArtifact(Artifact):
    """Video artifact"""
    stream_name: str
    end_timestamp: datetime

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

    @property
    def artifact_id(self) -> str:
        return f"{self.stream_name}_{int(self.timestamp.timestamp()*1000)}_{int(self.end_timestamp.timestamp()*1000)}"  # pylint: disable=line-too-long


@dataclass
class SnapshotArtifact(Artifact):
    """Snapshot artifact"""
    uuid: str

    @validator("recorder")
    def check_recorder(cls, value: RecorderType) -> RecorderType:
        """Validate recorder type"""
        if value not in [RecorderType.SNAPSHOT, RecorderType.INTERIOR_PREVIEW]:
            raise ValueError("Invalid recorder type")
        return value

    @property
    def artifact_id(self) -> str:
        uuid_without_ext = self.uuid.rstrip(Path(self.uuid).suffix)
        return f"{self.tenant_id}_{self.device_id}_{uuid_without_ext}_{int(self.timestamp.timestamp()*1000)}"  # pylint: disable=line-too-long


def parse_artifact(json_data: str) -> Artifact:
    """Parse artifact from string"""
    return parse_raw_as(Union[VideoArtifact, SnapshotArtifact], json_data)  # type: ignore
