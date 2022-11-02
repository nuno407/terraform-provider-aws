from datetime import timedelta
import logging
from typing import Any, Union
from base.processor import Processor

_logger = logging.getLogger('mdfparser.' + __name__)


class PersonCount(Processor):
    @property
    def name(self):
        return 'PersonCount'

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        max_person_count = self.calculate_person_count(synchronized_signals)
        return {'recording_overview': {
            'max_person_count': max_person_count
        }}

    def calculate_person_count(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> int:
        # create list with pc frames
        max_person_count = max([signals.get('PersonCount_value', 0) for signals in synchronized_signals.values()], default=0)
        _logger.info('Identified %s max Person count', max_person_count)
        return max_person_count
