from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union
from abc import ABC, abstractmethod


def to_relative_coord(abs_val: int, max_val: int) -> float:
    return abs_val / max_val


@dataclass
class KeyPoint():
    x: int
    y: int
    confidence: float
    name: str


@dataclass
class Person():
    keypoints: list[KeyPoint]
    name: Optional[str] = None


@dataclass
class BoundingBox():
    x: int
    y: int
    width: int
    height: int
    confidence: Optional[float] = None
    name: Optional[str] = None


@dataclass
class Classification():
    name: str
    value: Union[float, int]


@dataclass
class Frame():
    persons: list[Person]
    bboxes: list[BoundingBox]
    classifications: list[Classification]
    width: int
    height: int
    frame_number: Optional[int] = None
    utc_timestamp: Optional[datetime] = None


class FrameMetadataParser(ABC):
    @abstractmethod
    def parse(self) -> list[Frame]:
        raise NotImplementedError


class DataLoader(ABC):
    @abstractmethod
    def load(self, frame: list[Frame]):
        raise NotImplementedError

    # A map to specify the position of each keypoint
KEYPOINTS_SORTED = {
    "LeftAnkle": 0,
    "LeftEar": 1,
    "LeftElbow": 2,
    "LeftEye": 3,
    "LeftHip": 4,
    "LeftKnee": 5,
    "LeftShoulder": 6,
    "LeftWrist": 7,
    "Neck": 8,
    "Nose": 9,
    "RightAnkle": 10,
    "RightEar": 11,
    "RightElbow": 12,
    "RightEye": 13,
    "RightHip": 14,
    "RightKnee": 15,
    "RightShoulder": 16,
    "RightWrist": 17
}
