""" Artifact model. """
import json
import dataclasses
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


class ArtifactEncoder(json.JSONEncoder):
    """ JSON encoder for artifacts. """

    def default(self, o):
        if isinstance(o, Enum):
            return {"__enum__": str(o.value)}
        if isinstance(o, datetime):
            return {"__datetime__": o.isoformat()}
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return json.JSONEncoder.default(self, o)


class ArtifactDecoder(json.JSONDecoder):
    """ JSON decoder for artifacts. """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self,
                                  object_hook=self.dict_to_object,
                                  *args,
                                  **kwargs)

    def dict_to_object(self, dictionary):
        """ converts a dict to an object. """
        if "__enum__" in dictionary:
            return RecorderType(dictionary["__enum__"])
        if "__datetime__" in dictionary:
            return datetime.fromisoformat(dictionary["__datetime__"])
        return dictionary


@dataclass
class Artifact(ABC):
    """Generic artifact"""
    tenant_id: str
    device_id: str
    recorder: RecorderType
    timestamp: datetime

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return json.dumps(self, cls=ArtifactEncoder)


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
