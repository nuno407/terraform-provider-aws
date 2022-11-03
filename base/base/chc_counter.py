""" Counts the number and duration of Camera Health Check algorithm activations. """
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, TypedDict, Union, cast

from base.processor import Processor

_logger = logging.getLogger(__name__)


class ChcCounter(Processor):
    """ Counts the number and duration of Camera Health Check algorithm activations. """
    @property
    def name(self):
        return "ChcCounter"

    def _process(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> Dict[str, Any]:
        chc_periods = self.calculate_chc_periods(synchronized_signals)
        number_chc: int = len(chc_periods)
        length_chc: float = sum([period["duration"]
                                 for period in chc_periods], 0.0)
        return {"recording_overview": {
            "number_chc_events": number_chc,
            "chc_duration": length_chc
        }}

    class _ChcPeriod(TypedDict):
        frames: List[int]
        duration: float

    class _Frame(TypedDict, total=False):
        timestamp: timedelta
        interior_camera_health_response_cvb: Optional[float]
        interior_camera_health_response_cve: Optional[float]

    def calculate_chc_periods(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> List[  # pylint: disable=too-many-locals
            _ChcPeriod]:
        """Calculates periods where CHC was activated.

        Args:
            synchronized_signals (Dict[timedelta, Dict[str, Union[bool, int, float]]]): List of all received signals

        Returns:
            List[ _ChcPeriod]: List containinh the number of frames and duration of Camera Health Check activations.
        """
        # add a frame index to the synchronized data
        frames: Dict[int, ChcCounter._Frame] = {}
        for i, timestamp in enumerate(synchronized_signals.keys()):
            frame = ChcCounter._Frame(timestamp=timestamp)
            frame.update(
                cast(ChcCounter._Frame, synchronized_signals[timestamp]))
            frames[i] = frame

        # identify frames with cvb and cve greater or equal to 1
        frames_with_cv: List[int] = []
        for number, frame in frames.items():
            if ("interior_camera_health_response_cvb" in frame and
                    "interior_camera_health_response_cve" in frame and
                    (cast(float, frame["interior_camera_health_response_cvb"]) >= 1.0 or
                     cast(float, frame["interior_camera_health_response_cve"]) >= 1.0)):
                frames_with_cv.append(number)

        # group frames into events with tolerance
        frame_groups = self._group_frames_to_events(frames_with_cv, 2)

        # duration calculation
        chc_periods: List[ChcCounter._ChcPeriod] = []
        for frame_group in frame_groups:
            ts_first_frame: timedelta = frames[frame_group[0]]["timestamp"]
            ts_last_frame: timedelta = frames[frame_group[-1]]["timestamp"]
            if len(frames) > frame_group[-1] + 1:
                # there is a frame after last CHC frame, so add one frame to duration
                ts_end: timedelta = frames[frame_group[-1] + 1]["timestamp"]
            elif frame_group[-1] != 0:
                # there is no frame after last CHC frame, so add the previous frame length to duration
                frame_duration: timedelta = frames[frame_group[-1]
                                                   ]["timestamp"] - frames[frame_group[-1] - 1]["timestamp"]
                ts_end = ts_last_frame + frame_duration
            else:
                # there is only one frame at all, so just assume 1 second CHC
                ts_end = ts_last_frame + timedelta(seconds=1)
            duration = (ts_end - ts_first_frame).total_seconds()
            chc_period: ChcCounter._ChcPeriod = {
                "frames": frame_group, "duration": duration}
            chc_periods.append(chc_period)

        _logger.info("Identified %s CHC periods", len(chc_periods))

        return chc_periods

    def _group_frames_to_events(self, frames: List[int], tolerance: int) -> List[List[int]]:
        groups: List[List[int]] = []

        if len(frames) < 1:
            return groups

        entry = []
        for i, frame in enumerate(frames):
            entry.append(frame)
            if i == (len(frames) - 1) or abs(frames[i + 1] - frame) > tolerance:
                groups.append(entry)
                entry = []

        return groups
