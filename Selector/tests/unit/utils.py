import copy
from datetime import datetime, timedelta
from typing import Union

from pytz import UTC

from selector.model import PreviewMetadataV063
from selector.model.preview_metadata import (BoolObject, FloatObject, Frame,
                                             IntegerObject, Resolution, PtsTimeWindow, UtcTimeWindow)


class DataTestBuilder:
    def __init__(self) -> None:
        self._frame_sec: int = 1
        self._length: float = 1
        self._resolution = Resolution(width=640, height=320)
        self._segmentation_network_version = "segmentation"
        self._pose_network_version = "pose_network"
        self._cve_reference_file = "refernce_file"
        self._chunkPTS = PtsTimeWindow(pts_start=0, pts_end=23040)
        self._chunkUTC = UtcTimeWindow(utc_start=1687275042000, utc_end=1687275342000)
        self._metadata_version = "0.6.3"
        self._bool_attributes: dict[str, bool] = {}
        self._float_attributes: dict[str, float] = {}
        self._integer_attributes: dict[str, int] = {}

    def with_frames_per_sec(self, frame_per_sec: int) -> "DataTestBuilder":
        self._frame_sec = frame_per_sec
        return self

    def with_length(self, length: float) -> "DataTestBuilder":
        self._length = length
        return self

    def with_frame_count(self, frame_count: int) -> "DataTestBuilder":
        self._length = frame_count / self._framerate
        return self

    def with_metadata_version(self, metadata_version: str) -> "DataTestBuilder":
        self._metadata_version = metadata_version
        return self

    def with_bool_attribute(self, name: str, value: bool) -> "DataTestBuilder":
        self._bool_attributes[name] = value
        return self

    def with_float_attribute(self, name: str, value: float) -> "DataTestBuilder":
        self._float_attributes[name] = value
        return self

    def with_integer_attribute(self, name: str, value: int) -> "DataTestBuilder":
        self._integer_attributes[name] = value
        return self

    def build(self) -> PreviewMetadataV063:
        framecount = int(self._length * self._frame_sec)
        frames = []
        self._chunkUTC.start = self._chunkUTC.end - self._length * self._frame_sec * 1000
        self._chunkPTS.end = self._length * self._frame_sec * 1000

        objectlist: list[Union[FloatObject, BoolObject, IntegerObject]] = [
            BoolObject(bool_attributes=[self._bool_attributes]),
            FloatObject(float_attributes=[self._float_attributes]),
            IntegerObject(integer_attributes=[self._integer_attributes])
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
            chunk_pts=self._chunkPTS,
            chunk_utc=self._chunkUTC,
            frames=frames)
