""" Module that tests max Person Counter Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.max_person_count import MaxPersonCount


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestMaxPersonCounter:
    @fixture
    def max_person_count(self):
        return MaxPersonCount()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def simple_signals(self) -> dict:
        return {
            timedelta(seconds=5): {"PersonCount_value": 2},
            timedelta(seconds=15): {"PersonCount_value": 2},
            timedelta(seconds=16): {"PersonCount_value": 2},
            timedelta(seconds=20): {"PersonCount_value": 2},
            timedelta(seconds=20): {"something_else": 2}
        }

    @fixture
    def simple_signals_with_peak(self) -> dict:
        return {
            timedelta(seconds=5): {"PersonCount_value": 2},
            timedelta(seconds=15): {"PersonCount_value": 1},
            timedelta(seconds=16): {"PersonCount_value": 3},
            timedelta(seconds=20): {"PersonCount_value": 4}
        }

    def test_empty_signal_max_person_count(self, max_person_count: MaxPersonCount, empty_signal):
        # WHEN
        max_person_count_value = max_person_count.process(
            # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert max_person_count_value["recording_overview"]["max_person_count"] == 0

    def test_simple_signal_max_person_count(self, max_person_count: MaxPersonCount, simple_signals):
        # WHEN
        max_person_count_value = max_person_count.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals)

        # THEN
        assert max_person_count_value["recording_overview"]["max_person_count"] == 2

    def test_peak_simple_max_person_count(self, max_person_count: MaxPersonCount, simple_signals_with_peak):
        # WHEN
        max_person_count_value = max_person_count.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals_with_peak)

        # THEN
        assert max_person_count_value["recording_overview"]["max_person_count"] == 4
