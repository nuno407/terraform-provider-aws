""" model module. """
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Callable

_logger: logging.Logger = logging.getLogger(__name__)


class ArtifactType(Enum):
    """ artifact type enumerator. """
    FRONT_RECORDER = "FrontRecorder"
    INTERIOR_RECORDER = "InteriorRecorder"
    INTERIOR_PREVIEW = "InteriorRecorderPreview"
    TRAINING_RECORDER = "TrainingRecorder"
    SNAPSHOT = "Snapshot"
    UNKNOWN = "Unknown"


def debug_kinesis_sync(func: Callable):
    """Decorator to log the artifact_id before and after the sync"""
    @wraps(func)
    def wrapper(*args, **kwds):
        _logger.debug("kinesis sync - current artifact_id %s",
                      args[0].artifact_id)
        rtn = func(*args, **kwds)
        _logger.debug("kinesis sync - new artifact_id %s", args[0].artifact_id)
        return rtn
    return wrapper


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

    @abstractmethod
    def update_timestamps(self, database_video_id: str) -> None:
        """ Update timestamps from database_video_id """


@dataclass
class VideoArtifact(Artifact):
    """Video artifact message."""
    stream_name: str
    footage_from: datetime
    footage_to: datetime

    @property
    def artifact_id(self) -> str:
        return f"{self.stream_name}_{int(self.footage_from.timestamp()*1000)}_{int(self.footage_to.timestamp()*1000)}"  # pylint: disable=line-too-long

    @property
    def artifact_type(self) -> ArtifactType:
        if self.stream_name.endswith("InteriorRecorder"):  # pylint: disable=no-else-return
            return ArtifactType.INTERIOR_RECORDER
        elif self.stream_name.endswith("FrontRecorder"):
            return ArtifactType.FRONT_RECORDER
        elif self.stream_name.endswith("TrainingRecorder"):
            return ArtifactType.TRAINING_RECORDER
        elif self.stream_name.endswith("InteriorRecorderPreview"):
            return ArtifactType.INTERIOR_PREVIEW
        else:
            return ArtifactType.UNKNOWN

    @debug_kinesis_sync
    def update_timestamps(self, database_video_id: str) -> None:
        raw_footage_to = database_video_id.split("_")[-1]
        raw_footage_from = database_video_id.split("_")[-2]
        self.footage_from = datetime.fromtimestamp(
            int(raw_footage_from) / 1000)
        self.footage_to = datetime.fromtimestamp(int(raw_footage_to) / 1000)


@dataclass
class SnapshotArtifact(Artifact):
    """Snapshot artifact message."""
    uuid: str
    timestamp: datetime

    @property
    def artifact_id(self) -> str:
        uuid_no_format = self.uuid.rstrip(Path(self.uuid).suffix)
        return f"{self.tenant_id}_{self.device_id}_{uuid_no_format}_{int(self.timestamp.timestamp()*1000)}"  # pylint: disable=line-too-long

    @property
    def artifact_type(self) -> ArtifactType:
        return ArtifactType.SNAPSHOT

    @debug_kinesis_sync
    def update_timestamps(self, database_video_id: str) -> None:
        raw_timestamp = database_video_id.split("_")[-1]
        self.timestamp = datetime.fromtimestamp(int(raw_timestamp) / 1000)
        _logger.debug("kinesis sync - new timestamp %s",
                      self.timestamp.timestamp())
