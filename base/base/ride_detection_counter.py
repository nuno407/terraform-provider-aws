""" Module that counts number of rides. """
from datetime import timedelta
import logging
from typing import Any, Union
from base.processor import Processor

_logger = logging.getLogger(__name__)


class RideDetectionCounter(Processor):
    """ Class that counts number of rides. """
    @property
    def name(self):
        return "RideDetectionCounter"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        number_of_rides = self._count_ride_detection(synchronized_signals)
        return {"recording_overview": {
            "ride_detection_counter": number_of_rides
        }}

    def _count_ride_detection(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> int:
        number_of_rides = 0

        for item in synchronized_signals.values():
            if "RideInfo_people_count_before_value" in item or "RideInfo_people_count_after_value" in item:
                number_of_rides += 1

        _logger.info("Identified %s number of rides", number_of_rides)
        return number_of_rides
