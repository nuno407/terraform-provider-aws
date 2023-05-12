""" Module that reads people count before of a ride. """
from datetime import timedelta
import logging
from typing import Any, Union, cast
from base.processor import Processor

_logger = logging.getLogger(__name__)


class RideDetectionPeopleCountBefore(Processor):
    """ Class that reads people count before of a ride. """

    @property
    def name(self):
        return "RideDetectionPeopleCountBefore"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        people_count_before_value = self._get_ride_detection_people_count_before(synchronized_signals)
        return {"recording_overview": {
            "ride_detection_people_count_before": people_count_before_value
        }}

    def _get_ride_detection_people_count_before(self, synchronized_signals: dict[
            timedelta, dict[str, Union[bool, int, float]]]) -> int:
        people_count_before_value = -1

        people_count_before_list = []
        for signals in synchronized_signals.values():
            if "RideInfo_people_count_before_value" in signals:
                people_count_before_list.append(cast(int, signals.get("RideInfo_people_count_before_value", -1)))

        if people_count_before_list:
            people_count_before_value = people_count_before_list[-1]

        _logger.info("Identified %s ride detection people count", people_count_before_value)
        return people_count_before_value
