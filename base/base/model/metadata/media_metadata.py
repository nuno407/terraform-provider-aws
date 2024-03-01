# pylint: disable=too-few-public-methods
"""Media metadata that should be used for every metadata handling related to
TrainingRecorder, InteriorRecorder and TrainingMultiSnapshot

THIS IS NOT SUPPORTED FOR PREVIEW METADATA
(Due the way each classification is handled in the metadata)
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Generic, Iterator, Optional, TypeVar

from pydantic import Field, model_validator, AwareDatetime

from base.model.base_model import ConfiguredBaseModel
from base.model.metadata.base_metadata import (BaseFrame, BaseMetadata,
                                               FrameSignal, PtsTimeWindow,
                                               Resolution, UtcTimeWindow)


class BoundingBox(ConfiguredBaseModel):
    """
    Stores a detection bounding box with absolute coordinates and dimensions.
    """
    x: float = Field(alias="X")
    y: float = Field(alias="Y")
    confidence: float = Field(alias="Conf")
    width: int = Field(alias="Width")
    height: int = Field(alias="Height")


class MediaKeypointName(str, Enum):
    """ KeyPoints names in the metadata """
    LEFT_ANKLE = "LeftAnkle"
    LEFT_EAR = "LeftEar"
    LEFT_ELBOW = "LeftElbow"
    LEFT_EYE = "LeftEye"
    LEFT_HIP = "LeftHip"
    LEFT_KNEE = "LeftKnee"
    LEFT_SHOULDER = "LeftShoulder"
    LEFT_WRIST = "LeftWrist"
    NECK = "Neck"
    NOSE = "Nose"
    RIGHT_ANKLE = "RightAnkle"
    RIGHT_EAR = "RightEar"
    RIGHT_ELBOW = "RightElbow"
    RIGHT_EYE = "RightEye"
    RIGHT_HIP = "RightHip"
    RIGHT_KNEE = "RightKnee"
    RIGHT_SHOULDER = "RightShoulder"
    RIGHT_WRIST = "RightWrist"


KeyPointIDs = {keypoint_name: i for i, keypoint_name in enumerate(MediaKeypointName)}

T = TypeVar("T", float, int, bool, str)
KeyPointName = TypeVar("KeyPointName")


class MediaKeyPoint(ConfiguredBaseModel, Generic[KeyPointName]):
    """Keypoint"""
    name: KeyPointName = Field(alias="Name")
    x: float = Field(alias="X", default=0)
    y: float = Field(alias="Y", default=0)
    conf: float = Field(alias="Conf", default=0)


class Classification(ConfiguredBaseModel, Generic[T]):
    """Classification"""
    name: str
    value: T


class PersonDetails(ConfiguredBaseModel):
    """Pose containing person deails"""
    keypoints: list[MediaKeyPoint[MediaKeypointName]] = Field(alias="KeyPoint", default_factory=list)
    confidence: float = Field(alias="Confidence")


class ObjectList(ConfiguredBaseModel):
    """Object list from metadata"""
    bool_attributes: list[Classification[bool]] = Field(alias="boolAttributes", default_factory=list)
    float_attributes: list[Classification[float]] = Field(alias="floatAttributes", default_factory=list)
    string_attributes: list[Classification[str]] = Field(alias="stringAttributes", default_factory=list)
    integer_attributes: list[Classification[int]] = Field(alias="integerAttributes", default_factory=list)
    bbox_attributes: list[BoundingBox] = Field(alias="boundingBoxes", default_factory=list)
    person_details: list[PersonDetails] = Field(alias="personDetail", default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def flatten_objects(cls, data: Any) -> Any:
        """Flatten objects it ensures the attributes match the specified model"""

        # This allows the ObjectList to be parsed by a json already in the correct format
        if not isinstance(data, list):
            return data

        object_list: dict[str, list[Any]] = {
            "boolAttributes": [],
            "floatAttributes": [],
            "stringAttributes": [],
            "integerAttributes": [],
            "personDetail": [],
            "boundingBoxes": []
        }

        for obj in data:
            if not isinstance(obj, dict):
                raise ValueError("ObjectList should be a list of dictionaries")

            object_list["boolAttributes"].extend(obj.get("boolAttributes", []))
            object_list["floatAttributes"].extend(obj.get("floatAttributes", []))
            object_list["stringAttributes"].extend(obj.get("stringAttributes", []))
            object_list["integerAttributes"].extend(obj.get("integerAttributes", []))

            if "personDetail" in obj:
                person_details = obj.get("personDetail", {})
                person_details["confidence"] = obj.get("Confidence", 0)
                object_list["personDetail"].append(person_details)

        return object_list


class MediaFrame(BaseFrame):
    """Single frame of the metadata"""
    number: int
    timestamp: AwareDatetime
    object_list: ObjectList = Field(alias="objectlist")

    @model_validator(mode="before")
    @classmethod
    def flatten_objects(cls, data: Any) -> Any:
        """Flatten objects it ensures the attributes match the specified model"""

        # This allows a MediaFrame to be parsed by a json already in the correct format
        if "utc_timestamp_reference" not in data:
            return data

        delta_timestamp_pts = int(data["timestamp"]) - data["pts_timestamp_reference"]
        actual_timestamp = data["utc_timestamp_reference"] + delta_timestamp_pts
        data["timestamp"] = datetime.fromtimestamp(actual_timestamp / 1000, tz=timezone.utc)
        del data["utc_timestamp_reference"]
        del data["pts_timestamp_reference"]
        return data

    def get_numeric_classifications(self) -> Iterator[Classification[float]]:
        """Returns an iterator over all the numeric classifications in the frame"""
        for obj in self.object_list.float_attributes:
            yield obj

        for obj in self.object_list.integer_attributes:  # type: ignore
            yield Classification(name=obj.name, value=float(obj.value))

        for obj in self.object_list.bool_attributes:    # type: ignore
            yield Classification(name=obj.name, value=float(obj.value))

    def get_string(self, attribute_name: str) -> Optional[str]:
        """ Tries to get a string value from the frame """
        for obj in self.object_list.string_attributes:
            if obj.name == attribute_name:
                return obj.value

        return None

    def get_bool(self, attribute_name: str) -> Optional[bool]:
        """ Tries to get a boolean value from the frame """
        for obj in self.object_list.bool_attributes:
            if obj.name == attribute_name:
                return obj.value

        return None

    def get_float(self, attribute_name: str) -> Optional[float]:
        """ Tries to get a float value from the frame """
        for obj in self.object_list.float_attributes:
            if obj.name == attribute_name:
                return obj.value

        return None

    def get_integer(self, attribute_name: str) -> Optional[int]:
        """ Tries to get an integer value from the frame """
        for obj in self.object_list.integer_attributes:
            if obj.name == attribute_name:
                return obj.value

        return None


class MediaMetadata(BaseMetadata):
    """A single signal with it's corresponding utc timestamp"""
    resolution: Resolution
    metadata_version: str = Field(alias="metaData version", default=...)
    pose_network_version: str = Field(alias="PoseNetwork version", default=...)
    segmentation_network_version: str = Field(alias="SegmentationNetwork version", default=...)
    cve_reference_file: str = Field(alias="CVEReference file", default=...)
    chunk_pts: PtsTimeWindow = Field(alias="chunkPts")
    chunk_utc: UtcTimeWindow = Field(alias="chunkUtc")
    frames: list[MediaFrame] = Field(alias="frame", default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def store_context(cls, data: Any) -> Any:
        """
        Stores additional fields related to the timestamps
        in order for each frame be able to caculate it's utc timestamp
        """
        # This allows a MediaMetadata to be parsed by a json already in the correct format
        if "chunkUtc" not in data or not data["chunkUtc"]["utc_start"].isdigit():
            return data

        utc_timestamp_reference = int(data["chunkUtc"]["utc_start"])
        pts_timestamp_reference = int(data["chunkPts"]["pts_start"])

        for entry in data.get("frame", []):
            entry["utc_timestamp_reference"] = utc_timestamp_reference
            entry["pts_timestamp_reference"] = pts_timestamp_reference
        return data

    @property
    def footage_from(self) -> datetime:
        """
        Returns the footage_to datetime in UTC.
        """
        return self.chunk_utc.start

    @property
    def footage_to(self) -> datetime:
        """
        Returns the footage_to datetime in UTC.
        """
        return self.chunk_utc.end

    def get_string(self, name: str, default: Optional[str] = None) -> Iterator[FrameSignal]:
        """
        Returns an iterator for all string values in the metadata for the requested signal.
        If the value does not exist in a specific frame, the FrameSignal.value will be the on
        provided on the default argument.

        Args:
            name (str): The name of the signal to be requested
            default (str, optional): A default value in case a frame does not contain the signal.
                                    Defaults to None.

        Yields:
            Iterator[FrameSignal]: An iterator over the requested signal.
        """

        for frame in self.frames:
            val = frame.get_string(name)
            if val is None:
                yield FrameSignal(utc_time=frame.timestamp, value=default)
            else:
                yield FrameSignal(utc_time=frame.timestamp, value=val)

    def get_bool(self, name: str, default: Optional[bool] = None) -> Iterator[FrameSignal]:
        """
        Returns an iterator for all boolean values in the metadata for the requested signal.
        If the value does not exist in a specific frame, the FrameSignal.value will be the on
        provided on the default argument.

        Args:
            name (str): The name of the signal to be requested
            default (bool, optional): A default value in case a frame does not contain the signal.
                                    Defaults to None.

        Yields:
            Iterator[FrameSignal]: An iterator over the requested signal.
        """

        for frame in self.frames:
            val = frame.get_bool(name)
            if val is None:
                yield FrameSignal(utc_time=frame.timestamp, value=default)
            else:
                yield FrameSignal(utc_time=frame.timestamp, value=val)

    def get_float(self, name: str, default: Optional[float] = None) -> Iterator[FrameSignal]:
        """
        Returns an iterator for all float values in the metadata for the requested signal.
        If the value does not exist in a specific frame, the FrameSignal.value will be the on
        provided on the default argument.

        Args:
            name (str): The name of the signal to be requested
            default (float, optional): A default value in case a frame does not contain the signal.
                                    Defaults to None.

        Yields:
            Iterator[FrameSignal]: An iterator over the requested signal.
        """
        for frame in self.frames:
            val = frame.get_float(name)
            if val is None:
                yield FrameSignal(utc_time=frame.timestamp, value=default)
            else:
                yield FrameSignal(utc_time=frame.timestamp, value=val)

    def get_integer(self, name: str, default: Optional[int] = None) -> Iterator[FrameSignal]:
        """
        Returns an iterator for all int values in the metadata for the requested signal.
        If the value does not exist in a specific frame, the FrameSignal.value will be the on
        provided on the default argument.

        Args:
            name (str): The name of the signal to be requested
            default (int, optional): A default value in case a frame does not contain the signal.
                                    Defaults to None.

        Yields:
            Iterator[FrameSignal]: An iterator over the requested signal.
        """
        for frame in self.frames:
            val = frame.get_integer(name)
            if val is None:
                yield FrameSignal(utc_time=frame.timestamp, value=default)
            else:
                yield FrameSignal(utc_time=frame.timestamp, value=val)

    def get_frame_utc_timestamp(self, frame: BaseFrame) -> datetime:
        """
        Returns the frame UTC timestamp from a particular frame

        Args:
            frame (Frame): The Frame

        Returns:
            datetime: The UTC timestamp
        """
        if isinstance(frame, MediaFrame):
            return frame.timestamp
        raise ValueError("The frame is not of the correct type")
