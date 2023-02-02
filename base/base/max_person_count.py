""" Counts the maximum number of persons that has been detected during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union, cast

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class MaxPersonCount(Processor):
    """Processor that counts the maximum number of persons that has been detected during a ride."""
    @property
    def name(self):
        return "PersonCount"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        max_person_count = self._calculate_max_person_count(synchronized_signals)
        return {"recording_overview": {
            "max_person_count": max_person_count
        }}

    def _calculate_max_person_count(self,
                                    synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> int:
        # create list with pc frames
        max_person_count = max((cast(int, signals.get("PersonCount_value", 0))
                                for signals in synchronized_signals.values()), default=0)
        _logger.info("Identified %s max Person count", max_person_count)
        return max_person_count
