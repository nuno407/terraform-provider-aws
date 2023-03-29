""" Artifact model. """
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class RecorderType(Enum):
    """ artifact type enumerator. """
    FRONT = "FrontRecorder"
    INTERIOR = "InteriorRecorder"
    INTERIOR_PREVIEW = "InteriorRecorderPreview"
    TRAINING = "TrainingRecorder"
    SNAPSHOT = "TrainingMultiSnapshot"
    UNKNOWN = "Unknown"


@dataclass
class Artifact(ABC):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    recorder: RecorderType
    timestamp: datetime
    devcloud_id: str

@dataclass
class VideoArtifact(Artifact):
    """Video artifact"""
    stream_name: str
    end_timestamp: datetime

    @property
    def duration(self) -> float:
        """ duration of the video in seconds. """
        return (self.end_timestamp - self.timestamp).total_seconds()

@dataclass
class SnapshotArtifact(Artifact):
    """Snapshot artifact"""
    uuid: str
