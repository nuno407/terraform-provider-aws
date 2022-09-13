from datetime import datetime, timedelta
import logging
from typing import Any, Optional, TypedDict, Union, cast
from .processor import Processor

_logger = logging.getLogger('mdfparser.' + __name__)
class ChcCounter(Processor):
    @property
    def name(self):
        return 'ChcCounter'

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->dict[str, Any]:
        chc_periods = self.calculate_chc_periods(synchronized_signals)
        number_chc: int = len(chc_periods)
        length_chc: float = sum([period['duration'] for period in chc_periods], 0.0)
        return {'recording_overview':{
            'number_chc_events': number_chc,
            'chc_duration': length_chc
        }}

    class __ChcPeriod(TypedDict):
        frames: list[int]
        duration: float

    class __Frame(TypedDict, total=False):
        timestamp: timedelta
        interior_camera_health_response_cvb: Optional[float]
        interior_camera_health_response_cve: Optional[float]

    def calculate_chc_periods(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->list[__ChcPeriod]:
        ## add a frame index to the synchronized data
        frames: dict[int, ChcCounter.__Frame] = {}
        for i, timestamp in enumerate(synchronized_signals.keys()):
            frame = ChcCounter.__Frame(timestamp=timestamp)
            frame.update(cast(ChcCounter.__Frame, synchronized_signals[timestamp]))
            frames[i] = frame

        ## identify frames with cvb and cve equal to 1
        frames_with_cv: list[int] = []
        for number, frame in frames.items():
            if ('interior_camera_health_response_cvb' in frame and
                    'interior_camera_health_response_cve' in frame and
                    (frame["interior_camera_health_response_cvb"] == 1 or
                     frame["interior_camera_health_response_cve"] == 1)
                ):
                frames_with_cv.append(number)

         ## group frames into events with tolerance 
        frame_groups = self.__group_frames_to_events(frames_with_cv, 2)

        ## duration calculation
        chc_periods: list[ChcCounter.__ChcPeriod] = []
        for frame_group in frame_groups:
            ts_first_frame: timedelta = frames[frame_group[0]]['timestamp']
            ts_last_frame: timedelta = frames[frame_group[-1]]['timestamp']
            if(len(frames)>frame_group[-1]+1):
                #there is a frame after last CHC frame, so add one frame to duration
                ts_end: timedelta = frames[frame_group[-1]+1]['timestamp']
            elif frame_group[-1] != 0:
                #there is no frame after last CHC frame, so add the previous frame length to duration
                frame_duration: timedelta = frames[frame_group[-1]]['timestamp'] - frames[frame_group[-1]-1]['timestamp']
                ts_end = ts_last_frame + frame_duration
            else: 
                #there is only one frame at all, so just assume 1 second CHC
                ts_end = ts_last_frame + timedelta(seconds=1)
            duration = (ts_end - ts_first_frame).total_seconds()
            chc_period: ChcCounter.__ChcPeriod = {'frames': frame_group, 'duration': duration}
            chc_periods.append(chc_period)

        _logger.info('Identified %s CHC periods', len(chc_periods))

        return chc_periods

    def __group_frames_to_events(self, frames:list[int], tolerance:int)->list[list[int]]:
        groups: list[list[int]] = []

        if len(frames) < 1:
            return groups

        entry = []
        for i in range(0, len(frames)):
            entry.append(frames[i])
            if i == (len(frames) - 1) or abs(frames[i + 1] - frames[i]) > tolerance:
                groups.append(entry)
                entry = []

        return groups
