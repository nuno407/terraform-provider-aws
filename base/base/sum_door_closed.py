""" Counts the maximum number of persons that has been detected during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union, cast

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class SumDoorClosed(Processor):
    """Processor that counts all door closed events during a ride."""
    @property
    def name(self):
        return "SumDoorClosed"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        sum_door_closed = self._calculate_sum_door_closed(synchronized_signals)
        return {"recording_overview": {
            "sum_door_closed": sum_door_closed
        }}

    def _calculate_sum_door_closed(self,
                                   synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> int:
        # create list with pc frames
        door_closed = list(cast(int, signals.get("DoorClosedConfidence", None))
                           for signals in synchronized_signals.values())
        door_closed = [1 for i in door_closed if i is not None]
        sum_door_closed = sum(door_closed)
        _logger.info("Identified %s sum door closed", door_closed)
        return sum_door_closed
