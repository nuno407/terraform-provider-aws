""" Module that tests Ride Detection Counter Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.ride_detection_people_count_before import RideDetectionPeopleCountBefore
from base.ride_detection_people_count_after import RideDetectionPeopleCountAfter


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring
class TestRideDetectionCounter:
    @fixture
    def ride_detection_people_count_before(self):
        return RideDetectionPeopleCountBefore()

    @fixture
    def ride_detection_people_count_after(self):
        return RideDetectionPeopleCountAfter()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def simple_signals_with_rides(self) -> dict:
        return {
            timedelta(seconds=5): {"RideInfo_people_count_before_value": 2, "RideInfo_people_count_after_value": 3}
        }

    @fixture
    def signals_with_multiple_rides(self) -> dict:
        return {
            timedelta(seconds=5): {"RideInfo_people_count_before_value": 2, "RideInfo_people_count_after_value": 3},
            timedelta(seconds=15): {"RideInfo_people_count_before_value": 3, "RideInfo_people_count_after_value": 0},
            timedelta(seconds=16): {"something else": 20}
        }

    def test_empty_signal_ride_detection_people_count_before(
            self, ride_detection_people_count_before: RideDetectionPeopleCountBefore, empty_signal):
        # WHEN
        people_count_before_value = ride_detection_people_count_before.process(
            # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert people_count_before_value["recording_overview"]["ride_detection_people_count_before"] == -1

    def test_simple_person_count_before(self, ride_detection_people_count_before: RideDetectionPeopleCountBefore,
                                        simple_signals_with_rides):
        # WHEN
        people_count_before_value = ride_detection_people_count_before.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals_with_rides)

        # THEN
        assert people_count_before_value["recording_overview"]["ride_detection_people_count_before"] == 2

    def test_multiple_person_count_before(self, ride_detection_people_count_before: RideDetectionPeopleCountBefore,
                                          signals_with_multiple_rides):
        # WHEN
        people_count_before_value = ride_detection_people_count_before.process(
            # type: ignore # pylint: disable=protected-access
            signals_with_multiple_rides)

        # THEN

        assert people_count_before_value["recording_overview"]["ride_detection_people_count_before"] == 3

    def test_empty_signal_ride_detection_people_count_after(
            self, ride_detection_people_count_after: RideDetectionPeopleCountAfter, empty_signal):
        # WHEN
        people_count_after_value = ride_detection_people_count_after.process(
            # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert people_count_after_value["recording_overview"]["ride_detection_people_count_after"] == -1

    def test_simple_person_count_after(self, ride_detection_people_count_after: RideDetectionPeopleCountAfter,
                                       simple_signals_with_rides):
        # WHEN
        people_count_after_value = ride_detection_people_count_after.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals_with_rides)

        # THEN
        assert people_count_after_value["recording_overview"]["ride_detection_people_count_after"] == 3

    def test_multiple_person_count_after(self, ride_detection_people_count_after: RideDetectionPeopleCountAfter,
                                         signals_with_multiple_rides):
        # WHEN
        people_count_after_value = ride_detection_people_count_after.process(
            # type: ignore # pylint: disable=protected-access
            signals_with_multiple_rides)

        # THEN
        assert people_count_after_value["recording_overview"]["ride_detection_people_count_after"] == 0
