""" Calculates the variance of persons that has been detected during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union, cast

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class VariancePersonCount(Processor):
    """Processor that calculates the variance of persons that has been detected during a ride."""
    @property
    def name(self):
        return "VariancePersonCount"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        variance_person_count = self._calculate_variance_person_count(synchronized_signals)
        return {"recording_overview": {
            "variance_person_count": variance_person_count
        }}

    def _calculate_variance_person_count(
            self,
            synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> float:
        # create list with pc frames
        person_count = list(cast(int, signals.get("PersonCount_value", None))
                            for signals in synchronized_signals.values())
        person_count = [i for i in person_count if i is not None]

        if len(person_count) == 0:
            variance = float(0)
        else:
            mean = sum(person_count) / len(person_count)
            variance = sum((xi - mean) ** 2 for xi in person_count) / len(person_count)
            variance = round(variance, 2)
        _logger.info("Identified %s variance Person count", variance)
        return variance
