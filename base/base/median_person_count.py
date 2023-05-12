""" Counts the median of persons that has been detected during a ride. """
import logging
from datetime import timedelta
from statistics import median
from typing import Any, Union, cast

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class MedianPersonCount(Processor):
    """Processor that counts the median of persons that has been detected during a ride."""

    @property
    def name(self):
        return "PersonCount"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        median_person_count = self._calculate_median_person_count(synchronized_signals)
        return {"recording_overview": {
            "median_person_count": median_person_count
        }}

    def _calculate_median_person_count(self,
                                       synchronized_signals: dict[
                                           timedelta, dict[str, Union[bool, int, float]]]) -> float:
        person_count_values = [cast(int, signals.get("PersonCount_value", 0)) for signals in
                               synchronized_signals.values() if "PersonCount_value" in signals]
        if not person_count_values:
            return 0.0
        median_person_count = median(person_count_values)
        _logger.info("Identified %s median Person count", median_person_count)
        return median_person_count
