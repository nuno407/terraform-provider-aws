from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union
from abc import ABC, abstractmethod


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
    confidence: float
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


class DataLoader(ABC):
    """
    An interface to be implemented by any object that needs to load frame data.
    """
    @abstractmethod
    def load(self, frame: Frame):
        raise NotImplementedError
