"""Data Test Builder Module"""
import copy
import os
from unittest.mock import Mock
from datetime import timedelta, datetime, timezone
from typing import Union
from base.model.metadata.base_metadata import (BoolObject, FloatObject,
                                               IntegerObject, Resolution, PtsTimeWindow, UtcTimeWindow)
from base.testing.utils import load_relative_json_file
from selector.model import Frame
from selector.model.context import Context
from selector.model.ride_info import RideInfo
from selector.model.preview_metadata_63 import PreviewMetadataV063

DEFAULT_START_DATETIME = datetime(2023, 7, 3, 8, 38, 29, 461000, tzinfo=timezone.utc)
DEFAULT_END_DATETIME = datetime(2023, 7, 3, 8, 48, 29, 461000, tzinfo=timezone.utc)


class DataTestBuilder:  # pylint: disable=too-many-instance-attributes
    """DataTest Builder, use to produce artificial data for tests"""

    def __init__(self) -> None:
        """Constructor"""
        self._frame_sec: float = 1.0
        self._length_sec: float = 1.0
        self._resolution = Resolution(width=640, height=320)
        self._segmentation_network_version = "segmentation"
        self._pose_network_version = "pose_network"
        self._cve_reference_file = "reference_file"
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
        self._chunk_utc.start = self._chunk_utc.end - timedelta(seconds=self._length_sec)
        self._chunk_pts.end = int(self._length_sec * 1000)

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
                timestamp=i * self._frame_sec * 1000,
                timestamp64=i * self._frame_sec * 1000,
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


def get_rule_metadata(file_name: str) -> dict:
    """
    Retrieve a preview metadata from a file.
    The file should be located in the test_data folder.

    Args:
        file_name (str): The file name

    Returns:
        dict: The metadata in json format
    """
    return load_relative_json_file(__file__, os.path.join("test_data", file_name))


def build_context(metadata_file: str, ride_start: datetime = DEFAULT_START_DATETIME,
                  ride_end: datetime = DEFAULT_END_DATETIME) -> Context:
    """
    Build a context with a specific metadata file.

    Args:
        metadata_file (str): The metadata file name, should be located in the test_data folder.
        ride_start (datetime, optional): The time where the ride started. Defaults to DEFAULT_START_DATETIME.
        ride_end (datetime, optional): The time where the ride ended. Defaults to DEFAULT_END_DATETIME.

    Returns:
        Context: _description_
    """
    return Context(
        tenant_id="mock_tenant",
        device_id="mock_device",
        ride_info=RideInfo(
            start_ride=ride_start,
            end_ride=ride_end,
            preview_metadata=PreviewMetadataV063.model_validate(get_rule_metadata(metadata_file))
        ),
        recordings=Mock()
    )
