# pylint: disable=too-few-public-methods
"""Preview metadata that should be used for every rule"""
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional, Union

from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module


class ConfiguredBaseModel(BaseModel):
    """Base configuration for pydantic"""
    class Config:
        "Config for pydantic"
        allow_population_by_field_name = True
        extra = "allow"
        validate_assignment = True


class Resolution(ConfiguredBaseModel):
    """Resolution of the metadata"""
    width: int
    height: int


class StringObject(ConfiguredBaseModel):
    """String objects from metadata"""
    string_attributes: list[dict[str, str]] = Field(alias="stringAttributes", default=...)


class FloatObject(ConfiguredBaseModel):
    """Float objects from metadata"""
    float_attributes: list[dict[str, str]] = Field(alias="floatAttributes", default=...)


class BoolObject(ConfiguredBaseModel):
    """Bool objects from metadata"""
    bool_attributes: list[dict[str, str]] = Field(alias="boolAttributes", default=...)


class IntegerObject(ConfiguredBaseModel):
    """Integer objects from metadata"""
    integer_attributes: list[dict[str, str]] = Field(alias="integerAttributes", default=...)


ObjectList = list[Union[FloatObject, BoolObject, IntegerObject, StringObject]]


class PtsTimeWindow(ConfiguredBaseModel):
    """Window of pts timestamps"""
    start: int = Field(alias="pts_start")
    end: int = Field(alias="pts_end")


class UtcTimeWindow(ConfiguredBaseModel):
    """Window of utc timestamps"""
    start: int = Field(alias="utc_start")
    end: int = Field(alias="utc_end")


class KeyPoint(ConfiguredBaseModel):
    """Keypoint"""
    Name: str
    X: float
    Y: float
    Conf: float


class PersonDetail(ConfiguredBaseModel):
    """PersonDetails containing a list of keypoints"""
    key_point: list[KeyPoint] = Field(alias="KeyPoint", default_factory=list)


class Pose(ConfiguredBaseModel):
    """Pose containing person deails"""
    person_detail: PersonDetail = Field(alias="personDetail")


class Frame(ConfiguredBaseModel):
    """Single frame of the metadata"""
    number: int
    timestamp: int
    timestamp64: Optional[int]
    hasPoseList: Optional[bool]
    objectlist: ObjectList
    poselist: Optional[list[Pose]] = Field(default_factory=list)

    def get_string(self, attribute_name: str) -> Optional[str]:
        """ Tries to get a boolean value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, StringObject):
                for string_attribute_dict in oli.string_attributes:
                    if attribute_name in string_attribute_dict:
                        return string_attribute_dict[attribute_name]
        return None

    def get_bool(self, attribute_name: str) -> Optional[bool]:
        """ Tries to get a boolean value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, BoolObject):
                for bool_attribute_dict in oli.bool_attributes:
                    if attribute_name in bool_attribute_dict:
                        return bool(bool_attribute_dict[attribute_name])
        return None

    def get_float(self, attribute_name: str) -> Optional[float]:
        """ Tries to get a float value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, FloatObject):
                for float_attribute_dict in oli.float_attributes:
                    if attribute_name in float_attribute_dict:
                        return float(float_attribute_dict[attribute_name])
        return None

    def get_integer(self, attribute_name: str) -> Optional[int]:
        """ Tries to get an integer value from the frame """
        for oli in self.objectlist:
            if isinstance(oli, IntegerObject):
                for integer_attribute_dict in oli.integer_attributes:
                    if attribute_name in integer_attribute_dict:
                        return int(integer_attribute_dict[attribute_name])
        return None


@dataclass
class FrameSignal:
    """A single signal with it's corresponding utc timestamp"""
    utc_time: datetime
    value: Union[str, bool, float, int, None]


class PreviewMetadata(ConfiguredBaseModel):
    """An interface for the metadata that should be used in every rule"""
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
    def get_frame_utc_timestamp(self, frame: Frame) -> datetime:
        """
        Returns the frame UTC timestamp from a particular frame

        Args:
            frame (Frame): The Frame

        Returns:
            datetime: The UTC timestamp
        """
