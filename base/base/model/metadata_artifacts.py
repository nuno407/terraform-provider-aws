from datetime import datetime
from typing import Optional
from abc import ABC, abstractmethod
from pydantic import BaseModel


class KeyPoint(BaseModel):
    """
    Stores a key point using absolute coordinates.
    """
    x: int
    y: int
    confidence: float
    oclusion: Optional[bool]
    name: str


class Person(BaseModel):
    """
    Stores a person with their keypoints.
    """
    keypoints: list[KeyPoint]
    name: str


class BoundingBox(BaseModel):
    """
    Stores a detection bounding box with absolute coordinates and dimensions.
    """
    x: int
    y: int
    width: int
    height: int
    confidence: float
    name: str


class Classification(BaseModel):
    """
    Stores a classifcation, this can be any metric that can be represented as a float.
    """
    name: str
    value: float


class Frame(BaseModel):
    """
    Stores a Frame's metadata.
    It's important that every coordinate and dimension is stored as absolute values,
    in order for the loaders be able to process.
    """
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
