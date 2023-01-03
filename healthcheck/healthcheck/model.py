"""Healthcheck models module."""
from abc import ABC, abstractmethod
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from enum import Enum
from typing import NewType, Optional


class ArtifactType(Enum):
    """Artifact type."""
    FRONT_RECORDER = "FrontRecorder"
    INTERIOR_RECORDER = "InteriorRecorder"
    TRAINING_RECORDER = "TrainingRecorder"
    SNAPSHOT = "TrainingMultiSnapshot"
    UNKNOWN = "Unknow"


@dataclass
class S3Params():
    """AWS S3 parameters."""
    s3_bucket_anon: str
    s3_bucket_raw: str
    s3_dir: str


@dataclass
class Artifact(ABC):
    """Parsed artifact message."""
    tenant_id: str
    device_id: str

    @property
    @abstractmethod
    def artifact_type(self) -> ArtifactType:
        """Artifact type."""

    @property
    def internal_message_reference_id(self) -> str:
        """Compute hash for checking data completion"""
        return hashlib.sha256(self.artifact_id.encode("utf-8")).hexdigest()

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Artifact ID."""


@dataclass
class VideoArtifact(Artifact):
    """Video artifact message."""
    stream_name: str
    footage_from: datetime
    footage_to: datetime

    @property
    def artifact_id(self) -> str:
        return f"{self.tenant_id}_{self.stream_name}_{int(self.footage_from.timestamp()*1000)}_{int(self.footage_to.timestamp()*1000)}"

    @property
    def artifact_type(self) -> ArtifactType:
        if self.stream_name.endswith("InteriorRecorder"):
            return ArtifactType.INTERIOR_RECORDER
        elif self.stream_name.endswith("FrontRecorder"):
            return ArtifactType.FRONT_RECORDER
        elif self.stream_name.endswith("TrainingRecorder"):
            return ArtifactType.TRAINING_RECORDER
        else:
            return ArtifactType.UNKNOWN


@dataclass
class SnapshotArtifact(Artifact):
    """Snapshot artifact message."""
    uuid: str
    timestamp: datetime

    @property
    def artifact_id(self) -> str:
        uuid_no_format = self.uuid.rstrip(Path(self.uuid).suffix)
        return f"{self.tenant_id}_{self.device_id}_{uuid_no_format}_{int(self.timestamp.timestamp()*1000)}"

    @property
    def artifact_type(self) -> ArtifactType:
        return ArtifactType.SNAPSHOT


@dataclass
class MessageAttributes:
    """Message attributes."""
    tenant: Optional[str]
    device_id: Optional[str] = None


@dataclass
class SQSMessage:
    """SQS Message dataclass."""
    message_id: str
    receipt_handle: str
    timestamp: str
    body: dict
    attributes: MessageAttributes

    def stringify(self) -> str:
        """returns string JSON representation version of message

        Returns:
            str: JSON representation
        """
        return json.dumps(self, default=lambda o: o.__dict__)


DBDocument = NewType("DBDocument", dict)
