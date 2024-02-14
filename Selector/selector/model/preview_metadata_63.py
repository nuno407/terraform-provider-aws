"""Defines the structure of the PreviewMetadata for the version 0.6.3"""

from datetime import datetime, timedelta
from typing import Iterator, Optional

from pydantic import field_validator, Field
from base.model.metadata.base_metadata import Resolution, PtsTimeWindow, UtcTimeWindow, \
    BaseFrame, FrameSignal, Pose, ObjectList, StringObject, FloatObject, BoolObject, IntegerObject
from selector.model.preview_metadata import PreviewMetadata
from selector.constants import ALLOWED_METADATA_VERSIONS  # pylint: disable=unused-import


class Frame(BaseFrame):
    """The frame for the preview metadata"""
    hasPoseList: Optional[bool] = None
    poselist: Optional[list[Pose]] = Field(default_factory=list)
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


class PreviewMetadataV063(PreviewMetadata):
    """Preview Metadata for the version V063"""
    resolution: Resolution
    metadata_version: str = Field(alias="metaData version")
    pose_network_version: str = Field(alias="PoseNetwork version")
    segmentation_network_version: str = Field(
        alias="SegmentationNetwork version")
    cve_reference_file: str = Field(alias="CVEReference file")
    chunk_pts: PtsTimeWindow = Field(alias="chunkPts")
    chunk_utc: UtcTimeWindow = Field(alias="chunkUtc")
    frames: list[Frame] = Field(alias="frame", default_factory=list)

    @field_validator("metadata_version")
    @classmethod
    def check_metadata_version(cls, version):  # pylint: disable=no-self-argument
        """Ensure metadata version is expected"""
        if not isinstance(version, str):
            raise ValueError("Metadata does not contain a version")

        # if not any(map(version.startswith, ALLOWED_METADATA_VERSIONS)):
        #    raise ValueError(f"Metadata version {version} is not supported")

        return version

    @property
    def footage_from(self) -> datetime:
        """
        Returns the footage_from datetime in UTC.
        """
        return self.chunk_utc.start

    @property
    def footage_to(self) -> datetime:
        """
        Returns the footage_to datetime in UTC.
        """
        return self.chunk_utc.end

    def get_frame_utc_timestamp(self, frame: Frame) -> datetime:
        """
        Returns the utc timestamp of a specific frame.

        Args:
            frame (Frame): The frame

        Returns:
            datetime: The UTC timestamp of the frame
        """
        return self.chunk_utc.start + timedelta(milliseconds=frame.timestamp - self.chunk_pts.start)

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
            value = frame.get_bool(name)
            if value is not None:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=value)
            else:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=default)

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
            value = frame.get_float(name)
            if value is not None:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=value)
            else:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=default)

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
            value = frame.get_integer(name)
            if value is not None:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=value)
            else:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=default)

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
            value = frame.get_string(name)
            if value is not None:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=value)
            else:
                yield FrameSignal(utc_time=self.get_frame_utc_timestamp(frame), value=default)
