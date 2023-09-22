""" Synchronizer class for synchronizing MDF data with a recording. """
import logging
from datetime import timedelta
from typing import Any, Union

from base.timestamps import from_epoch_seconds_or_milliseconds

_logger = logging.getLogger("mdfparser." + __name__)


class Synchronizer:  # pylint: disable=too-few-public-methods
    """ Synchronizer class for synchronizing MDF data with a recording. """

    def synchronize(self, mdf: dict[str, Any], recording_epoch_from: int,  # pylint: disable=too-many-locals
                    recording_epoch_to: int) -> dict[timedelta, dict[str, Union[bool, float, int]]]:
        """Synchronizes MDF data with a recording. """
        _logger.debug("Synchronizing MDF from %s to %s", recording_epoch_from, recording_epoch_to)

        # check MDF format
        if "chunkUtc" not in mdf or "chunkPts" not in mdf or "frame" not in mdf:
            raise InvalidMdfException("Missing either chunkPts, chunkUtc or frame fields.")

        if "utc_start" not in mdf["chunkUtc"] or "utc_end" not in mdf["chunkUtc"]:
            raise InvalidMdfException("Missing either utc_start or utc_end in chunkUtc")

        if "pts_start" not in mdf["chunkPts"] or "pts_end" not in mdf["chunkPts"]:
            raise InvalidMdfException("Missing either pts_start or pts_end in chunkPts")

        pts_start = int(mdf["chunkPts"]["pts_start"])
        pts_end = int(mdf["chunkPts"]["pts_end"])
        epoch_start = int(mdf["chunkUtc"]["utc_start"])
        epoch_end = int(mdf["chunkUtc"]["utc_end"])
        recording_start = from_epoch_seconds_or_milliseconds(recording_epoch_from)
        recording_end = from_epoch_seconds_or_milliseconds(recording_epoch_to)

        # epoch calculation formula
        pts_to_epoch_factor = (epoch_end - epoch_start) / (pts_end - pts_start)

        # frames processing

        sync_frames = {}
        for frame in mdf["frame"]:

            # timestamp calculation and processing
            frame_timestamp_epoch = (int(frame.get("timestamp")) - pts_start) * \
                pts_to_epoch_factor + epoch_start  # pylint: disable=line-too-long
            frame_timestamp_absolute = from_epoch_seconds_or_milliseconds(frame_timestamp_epoch)

            if frame_timestamp_absolute < recording_start or frame_timestamp_absolute > recording_end:  # pylint: disable=line-too-long
                # skip frames of MDF that are outside the recording time
                continue

            # note that this timestamp will be relative to the beginning of the recording
            frame_timestamp = frame_timestamp_absolute - recording_start

            signals = self.__get_frame_signals(frame)

            if len(signals) > 0:
                sync_frames[frame_timestamp] = signals

        _logger.debug(
            "Finished synchronizing MDF from %s to %s",
            recording_epoch_from,
            recording_epoch_to)  # pylint: disable=line-too-long
        return sync_frames

    def __get_frame_signals(self, frame: dict[Any, Any]) -> dict[str, Union[bool, float, int]]:
        signals: dict[str, Union[bool, float, int]] = {}
        if "objectlist" in frame.keys():
            for item in frame["objectlist"]:
                signals.update(self.__extract_bools(item))
                signals.update(self.__extract_floats(item))
                signals.update(self.__extract_ints(item))
        return signals

    def __extract_bools(self, frame_objects: dict[Any, Any]) -> dict[str, bool]:
        values: dict[str, bool] = {}
        if "boolAttributes" in frame_objects:
            for attribute in frame_objects["boolAttributes"]:
                if "name" in attribute and "value" in attribute:
                    values[attribute["name"]] = (
                        attribute["value"] == "true")
        return values

    def __extract_floats(self, frame_objects: dict[Any, Any]) -> dict[str, float]:
        values: dict[str, float] = {}
        if "floatAttributes" in frame_objects:
            for attribute in frame_objects["floatAttributes"]:
                if "name" in attribute and "value" in attribute:
                    values[attribute["name"]] = float(attribute["value"])
        return values

    def __extract_ints(self, frame_objects: dict[Any, Any]) -> dict[str, int]:
        values: dict[str, int] = {}
        if "integerAttributes" in frame_objects:
            for attribute in frame_objects["integerAttributes"]:
                if "name" in attribute and "value" in attribute:
                    values[attribute["name"]] = int(attribute["value"])
        return values


class InvalidMdfException(Exception):
    """ Exception for invalid MDFs. """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
