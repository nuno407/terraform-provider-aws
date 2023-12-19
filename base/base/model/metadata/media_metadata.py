# pylint: disable=too-few-public-methods
"""Media metadata that should be used for every metadata handling related to
TrainingRecorder, InteriorRecorder and TrainingMultiSnapshot

THIS IS NOT SUPPORTED FOR PREVIEW METADATA
(Due the way each classification is handled in the metadata)
"""
from enum import Enum
from datetime import datetime
from typing import Generic, TypeVar, Any, Iterator, Optional
from base.model.base_model import ConfiguredBaseModel
from base.model.metadata.base_metadata import BaseMetadata, FrameSignal, BaseFrame, Resolution, UtcTimeWindow, PtsTimeWindow
from pydantic import Field, model_validator


class BoundingBox(ConfiguredBaseModel):
    """
    Stores a detection bounding box with absolute coordinates and dimensions.
    """
    x: float = Field(alias="X")
    y: float = Field(alias="Y")
    confidence: float = Field(alias="Conf")
    width: int = Field(alias="Width")
    height: int = Field(alias="Height")

class KeypointNames(str, Enum):
    LEFTANKLE ="LeftAnkle"
    LEFTEAR ="LeftEar"
    LEFTELBOW ="LeftElbow"
    LEFTEYE ="LeftEye"
    LEFTHIP ="LeftHip"
    LEFTKNEE ="LeftKnee"
    LEFTSHOULDER ="LeftShoulder"
    LEFTWRIST ="LeftWrist"
    NECK ="Neck"
    NOSE ="Nose"
    RIGHTANKLE ="RightAnkle"
    RIGHTEAR ="RightEar"
    RIGHTELBOW ="RightElbow"
    RIGHTEYE ="RightEye"
    RIGHTHIP ="RightHip"
    RIGHTKNEE ="RightKnee"
    RIGHTSHOULDER ="RightShoulder"
    RIGHTWRIST ="RightWrist"

KeyPointIDs = {keypoint_name:i for i, keypoint_name in enumerate(KeypointNames)}

T = TypeVar("T", float, int, bool, str)
KeyPointName = TypeVar("KeyPointName")


class MediaKeyPoint(ConfiguredBaseModel, Generic[KeyPointName]):
    """Keypoint"""
    name: KeyPointName = Field(alias="Name", default=KeyPointName)
    x: float = Field(alias="X", default=0)
    y: float = Field(alias="Y", default=0)
    conf: float = Field(alias="Conf", default = 0)


class Classification(ConfiguredBaseModel, Generic[T]):
    """Classification"""
    name: str
    value: T

class PersonDetails(ConfiguredBaseModel):
    """Pose containing person deails"""
    keypoints: list[MediaKeyPoint[KeypointNames]] = Field(alias="KeyPoint", default_factory=list)
    confidence: float = Field(alias="Confidence")

class ObjectList(ConfiguredBaseModel):
    """Object list from metadata"""
    bool_attributes: list[Classification[bool]] = Field(alias="boolAttributes", default_factory=[])
    float_attributes: list[Classification[float]] = Field(alias="floatAttributes", default_factory=[])
    string_attributes: list[Classification[str]] = Field(alias="stringAttributes", default_factory=[])
    integer_attributes: list[Classification[int]] = Field(alias="integerAttributes", default_factory=[])
    bbox_attributes: list[BoundingBox] = Field(alias="boundingBoxes", default_factory=[])
    person_details: list[PersonDetails] = Field(alias="personDetail", default_factory=[])

    @model_validator(mode="before")
    @classmethod
    def flatten_objects(cls, data: Any) -> Any:
        if not isinstance(data, list):
            raise ValueError("ObjectList should be a list")

        object_list = {
            "boolAttributes": [],
            "floatAttributes": [],
            "stringAttributes": [],
            "integerAttributes": [],
            "personDetail": []
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
    timestamp: datetime
    object_list: ObjectList = Field(alias="objectlist", default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def flatten_objects(cls, data: Any) -> Any:
        delta_timestamp_pts = int(data["timestamp"]) - data["pts_timestamp_reference"]
        actual_timestamp = data["utc_timestamp_reference"] + delta_timestamp_pts
        data["timestamp"] = datetime.fromtimestamp(actual_timestamp/1000)
        del data["utc_timestamp_reference"]
        del data["pts_timestamp_reference"]
        return data
    
    def get_numeric_classification(self) -> Iterator[Classification[float]]:
        """Returns an iterator over all the numeric classifications in the frame"""
        for obj in self.object_list.float_attributes:
            yield obj

        for obj in self.object_list.integer_attributes:
            yield Classification(name=obj.name, value=float(obj.value))

        for obj in self.object_list.bool_attributes:
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
    pose_network_version: str = Field(alias="SegmentationNetwork version", default=...)
    cve_reference_file: str = Field(alias="CVEReference file", default=...)
    chunk_pts: PtsTimeWindow = Field(alias="chunkPts", default=...)
    chunk_utc: UtcTimeWindow = Field(alias="chunkUtc", default=...)
    frames: list[MediaFrame] = Field(alias="frame", default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def store_context(cls, data: Any) -> Any:
        utc_timestamp_reference = int(data["chunkUtc"]["utc_start"])
        pts_timestamp_reference = int(data["chunkPts"]["pts_start"])

        for d in data["frame"]:
            d["utc_timestamp_reference"] = utc_timestamp_reference
            d["pts_timestamp_reference"] = pts_timestamp_reference
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
        else:
            raise ValueError("The frame is not of the correct type")
