""" Module that tests gnss coverage Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.gnss_coverage import GnssCoverage


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestMedianPersonCounter:
    @fixture
    def gnss_coverage(self):
        return GnssCoverage()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def simple_signals(self) -> dict:
        return {
            timedelta(seconds=5): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 2,
                                   "Gnss_horizontal_speed_accuracy": 2},
            timedelta(seconds=15): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 4,
                                    "Gnss_horizontal_speed_accuracy": 4},
            timedelta(seconds=16): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 6,
                                    "Gnss_horizontal_speed_accuracy": 6},
            timedelta(seconds=20): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 8,
                                    "Gnss_horizontal_speed_accuracy": 8}
        }

    @fixture
    def simple_signals_with_high_error(self) -> dict:
        return {
            timedelta(seconds=5): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 2,
                                   "Gnss_horizontal_speed_accuracy": 2},
            timedelta(seconds=15): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 4,
                                    "Gnss_horizontal_speed_accuracy": 4},
            timedelta(seconds=16): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 6,
                                    "Gnss_horizontal_speed_accuracy": 6},
            timedelta(seconds=20): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 8,
                                    "Gnss_horizontal_speed_accuracy": 150}
        }

    @fixture
    def simple_signals_with_less_satellites(self) -> dict:
        return {
            timedelta(seconds=5): {"Gnss_satellites_used": 0, "Gnss_horizontal_speed": 2,
                                   "Gnss_horizontal_speed_accuracy": 2},
            timedelta(seconds=15): {"Gnss_satellites_used": 1, "Gnss_horizontal_speed": 4,
                                    "Gnss_horizontal_speed_accuracy": 4},
            timedelta(seconds=16): {"Gnss_satellites_used": 1, "Gnss_horizontal_speed": 6,
                                    "Gnss_horizontal_speed_accuracy": 6},
            timedelta(seconds=20): {"Gnss_satellites_used": 5, "Gnss_horizontal_speed": 8,
                                    "Gnss_horizontal_speed_accuracy": 8}
        }

    def test_empty_signal_gnss_coverage(self, gnss_coverage: GnssCoverage, empty_signal):
        # WHEN
        gnss_coverage_value = gnss_coverage.process(  # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert gnss_coverage_value["recording_overview"]["gnss_coverage"] == 0

    def test_simple_signal_gnss_coverage(self, gnss_coverage: GnssCoverage, simple_signals):
        # WHEN
        gnss_coverage_value = gnss_coverage.process(  # type: ignore # pylint: disable=protected-access
            simple_signals)

        # THEN
        assert gnss_coverage_value["recording_overview"]["gnss_coverage"] == 1.0

    def test_high_error_signal_gnss_coverage(self, gnss_coverage: GnssCoverage, simple_signals_with_high_error):
        # WHEN
        gnss_coverage_value = gnss_coverage.process(  # type: ignore # pylint: disable=protected-access
            simple_signals_with_high_error)

        # THEN
        assert gnss_coverage_value["recording_overview"]["gnss_coverage"] == 0.75

    def test_less_satellites_signal_gnss_coverage(self, gnss_coverage: GnssCoverage,
                                                  simple_signals_with_less_satellites):
        # WHEN
        gnss_coverage_value = gnss_coverage.process(  # type: ignore # pylint: disable=protected-access
            simple_signals_with_less_satellites)

        # THEN
        assert gnss_coverage_value["recording_overview"]["gnss_coverage"] == 0.25
