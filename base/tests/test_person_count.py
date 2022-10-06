from datetime import timedelta

from pytest import fixture
from base.person_count import PersonCount

class TestPersonCounter:
    @fixture
    def person_count(self):
        return PersonCount()

    @fixture
    def simple_signals(self) -> dict:
        return {
        timedelta(seconds=5): {'PersonCount_value': 2},
        timedelta(seconds=15): {'PersonCount_value': 2},
        timedelta(seconds=16): {'PersonCount_value': 2},
        timedelta(seconds=20): {'PersonCount_value': 2}
        }

    @fixture
    def simple_signals_with_peak(self) -> dict:
        return {
            timedelta(seconds=5): {'PersonCount_value': 2},
            timedelta(seconds=15): {'PersonCount_value': 2},
            timedelta(seconds=16): {'PersonCount_value': 3},
            timedelta(seconds=20): {'PersonCount_value': 2}
        }

    def test_simple_signal_max_person_count(self, person_count: PersonCount, simple_signals):
        # WHEN
        average_person_count = person_count.calculate_person_count(simple_signals)

        # THEN
        assert(average_person_count == 2)

    def test_peak_simple_max_person_count(self, person_count: PersonCount, simple_signals_with_peak):
        # WHEN
        average_person_count = person_count.calculate_person_count(simple_signals_with_peak)

        # THEN
        assert(average_person_count == 3)