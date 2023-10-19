from datetime import datetime, timedelta
from typing import Iterator, Optional, Union
from pydantic import Field
from base.model.base_model import ConfiguredBaseModel
from base.model.metadata.base_metadata import BaseFrame, BaseMetadata, FrameSignal, ObjectList, PersonDetail, PtsTimeWindow, Resolution, UtcTimeWindow


class Person(ConfiguredBaseModel):
    id: int
    confidence: float
    person_detail: PersonDetail = Field(alias="personDetail")


class SnapshotFrame(BaseFrame):
    objectlist: list[Union[ObjectList, Person]]


class SnapshotMetadata(BaseMetadata):
    resolution: Resolution
    metadata_version: str = Field(alias="metaData version")
    pose_network_version: str = Field(alias="PoseNetwork version")
    segmentation_network_version: str = Field(alias="SegmentationNetwork version")
    cve_reference_file: str = Field(alias="CVEReference file")
    chunk_pts: PtsTimeWindow = Field(alias="chunkPts")
    chunk_utc: UtcTimeWindow = Field(alias="chunkUtc")
    frames: list[SnapshotFrame] = Field(alias="frame", default_factory=list)

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

    def get_frame_utc_timestamp(self, frame: SnapshotFrame) -> datetime:
        """
        Returns the utc timestamp of a specific frame.

        Args:
            frame (Frame): The frame

        Returns:
            datetime: The UTC timestamp of the frame
        """
        return self.chunk_utc.start + timedelta(milliseconds=(frame.timestamp - self.chunk_pts.start))

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
