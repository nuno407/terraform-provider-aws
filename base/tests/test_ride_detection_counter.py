from datetime import timedelta

from pytest import fixture
from base.ride_detection_counter import RideDetectionCounter


class TestRideDetectionCounter:
    @fixture
    def ride_detection_counter(self):
        return RideDetectionCounter()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def simple_signals_with_rides(self) -> dict:
        return {
            timedelta(seconds=5): {'RideInfo_people_count_before_value': 2},
            timedelta(seconds=15): {'RideInfo_people_count_before_value': 3}
        }

    def test_empty_signal_ride_detection_counter(self, ride_detection_counter: RideDetectionCounter, empty_signal):
        # WHEN
        number_of_rides = ride_detection_counter.count_ride_detection(empty_signal)

        # THEN
        assert (number_of_rides == 0)

    def test_peak_simple_max_person_count(self, ride_detection_counter: RideDetectionCounter,
                                          simple_signals_with_rides):
        # WHEN
        number_of_rides = ride_detection_counter.count_ride_detection(simple_signals_with_rides)

        # THEN
        assert (number_of_rides == 2)
