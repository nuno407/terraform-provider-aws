""" Module that tests Person Counter Processor. """
from datetime import timedelta

from pytest import fixture

from base.sum_door_closed import SumDoorClosed


# pylint: disable=missing-function-docstring,missing-class-docstring
class TestSumDoorClosed:
    @fixture
    def sum_door_closed(self):
        return SumDoorClosed()

    @fixture
    def empty_signal_sum_door_close(self) -> dict:
        return {
        }

    @fixture
    def few_door_closed_events(self) -> dict:
        return {
            timedelta(seconds=5): {"DoorClosedConfidence": 0.1, "something_else": 100},
            timedelta(seconds=15): {"something_else": 0},
            timedelta(seconds=16): {"something_else": 0},
            timedelta(seconds=20): {"DoorClosedConfidence": 0.5},
            timedelta(seconds=21): {"something_else": 100}
        }

    @fixture
    def many_door_closed_events(self) -> dict:
        return {
            timedelta(seconds=5): {"DoorClosedConfidence": 0.1, "something_else": 100},
            timedelta(seconds=15): {"DoorClosedConfidence": 0.1},
            timedelta(seconds=16): {"DoorClosedConfidence": 0.1},
            timedelta(seconds=20): {"something_else": 100},
            timedelta(seconds=21): {"DoorClosedConfidence": 0.5}
        }

    def test_sum_of_empty_signal_close_door_events(self, sum_door_closed: SumDoorClosed, empty_signal_sum_door_close: dict):
        # WHEN
        sum_door_closed_value = sum_door_closed._calculate_sum_door_closed(  # type: ignore # pylint: disable=protected-access
            empty_signal_sum_door_close)

        # THEN
        assert sum_door_closed_value == 0

    def test_sum_of_few_close_door_events(self, sum_door_closed: SumDoorClosed, few_door_closed_events: dict):
        # WHEN
        sum_door_closed_value = sum_door_closed._calculate_sum_door_closed(  # type: ignore # pylint: disable=protected-access
            few_door_closed_events)

        # THEN
        assert sum_door_closed_value == 2

    def test_sum_of_many_close_door_events(self, sum_door_closed: SumDoorClosed, many_door_closed_events: dict):
        # WHEN
        sum_door_closed_value = sum_door_closed._calculate_sum_door_closed(  # type: ignore # pylint: disable=protected-access
            many_door_closed_events)

        # THEN
        assert sum_door_closed_value == 4
