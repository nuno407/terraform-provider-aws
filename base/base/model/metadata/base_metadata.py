# pylint: disable=too-few-public-methods
"""Base metadata that should be used for every metadata handling"""
from abc import abstractmethod
from datetime import datetime
from typing import Annotated, Iterator, Optional, Union, TypeVar

from pydantic import ConfigDict, BaseModel, Field
from pydantic.functional_validators import BeforeValidator

from base.model.validators import UtcDatetimeInPast  # pylint: disable=no-name-in-module

from base.model.base_model import ConfiguredBaseModel


class Resolution(ConfiguredBaseModel):
    """Resolution of the metadata"""
    width: int
    height: int


def parse_metadata_attributes(value) -> dict:
    """Parse metadata attributes"""
    attributes = {}
    if isinstance(value, list):
        for val in value:
            attributes.update(parse_metadata_attributes(val))
    elif isinstance(value, dict):
        if "name" in value and "value" in value:
            attributes[value["name"]] = value["value"]
        else:
            attributes = value
    return attributes


T = TypeVar("T", float, int, bool, str)
Attributes = Annotated[dict[str, T], BeforeValidator(parse_metadata_attributes)]


class StringObject(ConfiguredBaseModel):
    """String objects from metadata"""
    string_attributes: Attributes[str] = Field(alias="stringAttributes", default=...)


class FloatObject(ConfiguredBaseModel):
    """Float objects from metadata"""
    float_attributes: Attributes[float] = Field(alias="floatAttributes", default=...)


class BoolObject(ConfiguredBaseModel):
    """Bool objects from metadata"""
    bool_attributes: Attributes[bool] = Field(alias="boolAttributes", default=...)


class IntegerObject(ConfiguredBaseModel):
    """Integer objects from metadata"""
    integer_attributes: Attributes[int] = Field(alias="integerAttributes", default=...)


ObjectList = Union[FloatObject, BoolObject, IntegerObject, StringObject]


class PtsTimeWindow(ConfiguredBaseModel):
    """Window of pts timestamps"""
    start: int = Field(alias="pts_start")
    end: int = Field(alias="pts_end")


class UtcTimeWindow(ConfiguredBaseModel):
    """Window of utc timestamps"""
    start: UtcDatetimeInPast = Field(alias="utc_start")
    end: UtcDatetimeInPast = Field(alias="utc_end")


class KeyPoint(ConfiguredBaseModel):
    """Keypoint"""
    name: str = Field(alias="Name")
    x: float = Field(alias="X")
    y: float = Field(alias="Y")
    conf: float = Field(alias="Conf")


class PersonDetail(ConfiguredBaseModel):
    """PersonDetails containing a list of keypoints"""
    key_point: list[KeyPoint] = Field(alias="KeyPoint", default_factory=list)


class Pose(ConfiguredBaseModel):
    """Pose containing person deails"""
    person_detail: PersonDetail = Field(alias="personDetail")


class BaseFrame(ConfiguredBaseModel):
    """Single frame of the metadata"""
    number: int
    timestamp: int
    timestamp64: Optional[int] = None
    objectlist: list[ObjectList]

    def get_string(self, attribute_name: str) -> Optional[str]:
        """ Tries to get a boolean value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, StringObject):
                return oli.string_attributes.get(attribute_name, None)
        return None

    def get_bool(self, attribute_name: str) -> Optional[bool]:
        """ Tries to get a boolean value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, BoolObject):
                return oli.bool_attributes.get(attribute_name, None)
        return None

    def get_float(self, attribute_name: str) -> Optional[float]:
        """ Tries to get a float value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, FloatObject):
                return oli.float_attributes.get(attribute_name, None)
        return None

    def get_integer(self, attribute_name: str) -> Optional[int]:
        """ Tries to get an integer value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, IntegerObject):
                return oli.integer_attributes.get(attribute_name, None)
        return None


class FrameSignal(ConfiguredBaseModel):
    """A single signal with it's corresponding utc timestamp"""
    utc_time: datetime
    value: Union[str, bool, float, int, None]


class BaseMetadata(ConfiguredBaseModel):
    """An interface for the metadata that should be used everywhere it's needed"""
    @property
    @abstractmethod
    def footage_from(self) -> datetime:
        """
        Returns the footage_to datetime in UTC.
        """

    @property
    @abstractmethod
    def footage_to(self) -> datetime:
        """
        Returns the footage_to datetime in UTC.
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def get_frame_utc_timestamp(self, frame: BaseFrame) -> datetime:
        """
        Returns the frame UTC timestamp from a particular frame

        Args:
            frame (Frame): The Frame

        Returns:
            datetime: The UTC timestamp
        """
