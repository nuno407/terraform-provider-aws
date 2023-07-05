"""Data Test Builder Module"""
import copy
from typing import Union

from selector.model import PreviewMetadataV063
from selector.model.preview_metadata import (BoolObject, FloatObject, Frame,
                                             IntegerObject, Resolution, PtsTimeWindow, UtcTimeWindow)


class DataTestBuilder:  # pylint: disable=too-many-instance-attributes
    """DataTest Builder, use to produce artificial data for tests"""

    def __init__(self) -> None:
        """Constructor"""
        self._frame_sec: float = 1.0
        self._length_sec: float = 1.0
        self._resolution = Resolution(width=640, height=320)
        self._segmentation_network_version = "segmentation"
        self._pose_network_version = "pose_network"
        self._cve_reference_file = "refernce_file"
        self._chunk_pts = PtsTimeWindow(start=0, end=23040)
        self._chunk_utc = UtcTimeWindow(
            start=1687275042000, end=1687275342000)
        self._metadata_version = "0.6.3"
        self._bool_attributes: dict[str, bool] = {}
        self._float_attributes: dict[str, float] = {}
        self._integer_attributes: dict[str, int] = {}

    def with_frames_per_sec(self, frame_per_sec: float) -> "DataTestBuilder":
        """
        Set's a specific frame per second rate.

        Args:
            frame_per_sec (int): Number of frames that should be created per second.

        Returns:
            DataTestBuilder: Returns the builder
        """
        self._frame_sec = frame_per_sec
        return self

    def with_length(self, length_sec: float) -> "DataTestBuilder":
        """
        Set's a specific length for the test data in seconds.

        Args:
            length_sec (int): The length in seconds of the video

        Returns:
            DataTestBuilder: Returns the builder
        """
        self._length_sec = length_sec
        return self

    def with_frame_count(self, frame_count: int) -> "DataTestBuilder":
        """
        Set's a specific number of frame for the test data.

        Args:
            frame_count (int): The number of desired frames
        Returns:
            DataTestBuilder: Returns the builder
        """
        self._length_sec = frame_count / self._frame_sec
        return self

    def with_metadata_version(self, metadata_version: str) -> "DataTestBuilder":
        """
        Set's a specific metadata version for the test data.

        Args:
            metadata_version (int): The desired version
        Returns:
            DataTestBuilder: Returns the builder
        """
        self._metadata_version = metadata_version
        return self

    def with_bool_attribute(self, name: str, value: bool) -> "DataTestBuilder":
        """
        Set's a specific bool attribute for every frame.

        Args:
            value (bool): The boolean value
        Returns:
            DataTestBuilder: Returns the builder
        """
        self._bool_attributes[name] = value
        return self

    def with_float_attribute(self, name: str, value: float) -> "DataTestBuilder":
        """
        Set's a specific float attribute for every frame.

        Args:
            value (float): The float value
        Returns:
            DataTestBuilder: Returns the builder
        """
        self._float_attributes[name] = value
        return self

    def with_integer_attribute(self, name: str, value: int) -> "DataTestBuilder":
        """
        Set's a specific integer attribute for every frame.

        Args:
            value (float): The integer value
        Returns:
            DataTestBuilder: Returns the builder
        """
        self._integer_attributes[name] = value
        return self

    def build(self) -> PreviewMetadataV063:
        """
        The builder to build the metadata

        Returns:
            PreviewMetadataV063: _description_
        """
        framecount = int(self._length_sec * self._frame_sec)
        frames = []
        self._chunk_utc.start = int(
            self._chunk_utc.end - self._length_sec * self._frame_sec * 1000)
        self._chunk_pts.end = int(self._length_sec * self._frame_sec * 1000)

        objectlist: list[Union[FloatObject, BoolObject, IntegerObject]] = [
            # type: ignore
            BoolObject(bool_attributes=[self._bool_attributes]),
            # type: ignore
            FloatObject(float_attributes=[self._float_attributes]),
            IntegerObject(integer_attributes=[
                          self._integer_attributes])  # type: ignore
        ]
        for i in range(framecount):
            frame = Frame(
                number=i,
                timestamp=i * 1000,
                timestamp64=i * 1000,
                objectlist=copy.deepcopy(objectlist)
            )
            frames.append(frame)

        return PreviewMetadataV063(
            resolution=self._resolution,
            pose_network_version=self._pose_network_version,
            metadata_version=self._metadata_version,
            segmentation_network_version=self._segmentation_network_version,
            cve_reference_file=self._cve_reference_file,
            chunk_pts=self._chunk_pts,
            chunk_utc=self._chunk_utc,
            frames=frames)
