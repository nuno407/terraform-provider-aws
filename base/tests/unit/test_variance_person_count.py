""" Module that tests Person Counter Processor. """
from datetime import timedelta

from pytest import fixture

from base.variance_person_count import VariancePersonCount


# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestVariancePersonCounter:
    @fixture
    def variance_person_count(self):
        return VariancePersonCount()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def pc_low_variance(self) -> dict:
        return {
            timedelta(seconds=5): {"PersonCount_value": 2},
            timedelta(seconds=15): {"PersonCount_value": 2},
            timedelta(seconds=16): {"PersonCount_value": 2},
            timedelta(seconds=20): {"PersonCount_value": 2},
            timedelta(seconds=21): {"something_else": 100}
        }

    @fixture
    def pc_high_variance(self) -> dict:
        return {
            timedelta(seconds=5): {"PersonCount_value": 2},
            timedelta(seconds=15): {"PersonCount_value": 1},
            timedelta(seconds=16): {"PersonCount_value": 4},
            timedelta(seconds=20): {"PersonCount_value": 3},
            timedelta(seconds=21): {"something_else": 100}
        }

    def test_empty_signal_max_person_count(self, variance_person_count: VariancePersonCount, empty_signal: dict):
        # WHEN
        variance_person_count_value = variance_person_count._calculate_variance_person_count(  # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert variance_person_count_value == 0

    def test_pc_low_variance_max_person_count(self, variance_person_count: VariancePersonCount, pc_low_variance: dict):
        # WHEN
        variance_person_count_value = variance_person_count._calculate_variance_person_count(  # type: ignore # pylint: disable=protected-access
            pc_low_variance)

        # THEN
        assert variance_person_count_value == 0

    def test_pc_high_variance_max_person_count(self,
                                               variance_person_count: VariancePersonCount,
                                               pc_high_variance: dict):
        # WHEN
        variance_person_count_value = variance_person_count._calculate_variance_person_count(  # type: ignore # pylint: disable=protected-access
            pc_high_variance)

        # THEN
        assert variance_person_count_value == 1.25
