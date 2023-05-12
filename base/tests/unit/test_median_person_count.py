""" Module that tests median Person Counter Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.median_person_count import MedianPersonCount


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestMedianPersonCounter:
    @fixture
    def median_person_count(self):
        return MedianPersonCount()

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
            timedelta(seconds=5): {"PersonCount_value": 1},
            timedelta(seconds=15): {"PersonCount_value": 3},
            timedelta(seconds=16): {"PersonCount_value": 3},
            timedelta(seconds=17): {"PersonCount_value": 4},
            timedelta(seconds=20): {"PersonCount_value": 1}
        }

    def test_empty_signal_median_person_count(self, median_person_count: MedianPersonCount, empty_signal):
        # WHEN
        median_person_count_value = median_person_count.process(
            # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert median_person_count_value["recording_overview"]["median_person_count"] == 0

    def test_simple_signal_median_person_count(self, median_person_count: MedianPersonCount, simple_signals):
        # WHEN
        median_person_count_value = median_person_count.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals)

        # THEN
        assert median_person_count_value["recording_overview"]["median_person_count"] == 2

    def test_peak_simple_median_person_count(self, median_person_count: MedianPersonCount, simple_signals_with_peak):
        # WHEN
        median_person_count_value = median_person_count.process(
            # type: ignore # pylint: disable=protected-access
            simple_signals_with_peak)

        # THEN
        assert median_person_count_value["recording_overview"]["median_person_count"] == 3
