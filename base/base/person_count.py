from datetime import datetime, timedelta
import logging
from typing import Any, Optional, TypedDict, Union, cast
from base.processor import Processor

_logger = logging.getLogger('mdfparser.' + __name__)
class PersonCount(Processor):
    @property
    def name(self):
        return 'PersonCount'

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->dict[str, Any]:
        max_person_count = self.calculate_person_count(synchronized_signals)
        return {'recording_overview':{
            'max_person_count': max_person_count
        }}

    class __Frame(TypedDict, total=False):
        timestamp: timedelta

    def calculate_person_count(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])-> int:
        ## add a frame index to the synchronized data
        frames: dict[int, PersonCount.__Frame] = {}
        for i, timestamp in enumerate(synchronized_signals.keys()):
            frame = PersonCount.__Frame(timestamp=timestamp)
            frame.update(cast(PersonCount.__Frame, synchronized_signals[timestamp]))
            frames[i] = frame

        ## identify frames with cvb and cve equal to 1
        frames_with_cv: list[int] = []
        for number, frame in frames.items():
            if ('PersonCount_value' in frame):
                frames_with_cv.append(frame["PersonCount_value"])

        max_person_count = max(frames_with_cv)
        _logger.info('Identified %s max Person count', max_person_count)
        return max_person_count