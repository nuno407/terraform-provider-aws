""" Calculates the gnss coverage during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class GnssCoverage(Processor):
    """Processor that calculates the coverage of the GNSS signal used during a ride."""
    @property
    def name(self):
        return "GnssCoverage"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        gnss_coverage = self._calculate_gnss_coverage(synchronized_signals)
        return {"recording_overview": {
            "gnss_coverage": gnss_coverage
        }}

    def _calculate_gnss_coverage(
            self,
            synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> float:
        # create list with pc frames
        gnss_satellites_used = [
            signals.get("Gnss_satellites_used", 0) for signals in synchronized_signals.values()
        ]
        gnss_horizontal_speed_accuracy = [
            signals.get("Gnss_horizontal_speed_accuracy", 0) for signals in synchronized_signals.values()
        ]
        gnss_used = [
            gnss_satellites_used[i] >= 4 and gnss_horizontal_speed_accuracy[i] < 45
            for i in range(len(gnss_satellites_used))
        ]
        valid_signals_count = sum(1 for used in gnss_used if used)
        if not any(gnss_used):
            gnss_coverage_value = 0.0
        else:
            gnss_coverage_value = valid_signals_count / sum(1 for _ in gnss_used)
        _logger.info("Identified %s coverage of gnss used", gnss_coverage_value)
        return gnss_coverage_value
