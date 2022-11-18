""" Module to test CHC Counter. """
from datetime import timedelta

from pytest import fixture, mark

from base.chc_counter import ChcCounter


# pylint: disable=missing-function-docstring,missing-class-docstring
class TestChcCounter:
    @fixture
    def chc_counter(self):
        return ChcCounter()

    @mark.parametrize(
        "simple_signals",
        [
            {
                timedelta(seconds=5): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 0
                },
                timedelta(seconds=15): {
                    "interior_camera_health_response_cvb": 1,
                    "interior_camera_health_response_cve": 0
                },
                timedelta(seconds=16): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 1
                },
                timedelta(seconds=20): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 0
                }
            },
            {
                timedelta(seconds=5): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 0
                },
                timedelta(seconds=15): {
                    "interior_camera_health_response_cvb": 1,
                    "interior_camera_health_response_cve": 0
                },
                timedelta(seconds=16): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 2
                },
                timedelta(seconds=20): {
                    "interior_camera_health_response_cvb": 0,
                    "interior_camera_health_response_cve": 0
                }
            }
        ]
    )
    def test_calculate_chc_periods(self, chc_counter: ChcCounter, simple_signals):
        # WHEN
        chc_periods = chc_counter.calculate_chc_periods(simple_signals)

        # THEN
        assert len(chc_periods) == 1
        assert chc_periods[0]["duration"] == 5.0
        assert chc_periods[0]["frames"] == [1, 2]

    @fixture
    def signals_with_gap(self) -> dict:
        return {
            timedelta(seconds=5): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 0},
            timedelta(seconds=15): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 0},
            timedelta(seconds=16): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 1},
            timedelta(seconds=17): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 0},
            timedelta(seconds=18): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 1},
            timedelta(seconds=19): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 1},
            timedelta(seconds=20): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 0},
            timedelta(seconds=21): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 0},
            timedelta(seconds=22): {"interior_camera_health_response_cvb": 0, "interior_camera_health_response_cve": 0},
            timedelta(seconds=23): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 0},
            timedelta(seconds=24): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 0},
            timedelta(seconds=25): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 0},
        }

    def test_calculate_chc_periods_with_gap(self, chc_counter: ChcCounter, signals_with_gap):
        # WHEN
        chc_periods = chc_counter.calculate_chc_periods(signals_with_gap)

        # THEN
        assert len(chc_periods) == 2
        assert chc_periods[0]["duration"] == 5.0
        assert chc_periods[0]["frames"] == [1, 2, 4, 5]

    def test_process(self, chc_counter: ChcCounter, signals_with_gap):
        # WHEN
        result = chc_counter.process(signals_with_gap)

        # THEN
        recording_overview = result["recording_overview"]
        assert recording_overview["number_chc_events"] == 2
        assert recording_overview["chc_duration"] == 8.0

    @fixture
    def only_chc_true(self) -> dict:
        return {
            timedelta(seconds=5): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 1},
            timedelta(seconds=10): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 1},
            timedelta(seconds=15): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 1},
            timedelta(seconds=20): {"interior_camera_health_response_cvb": 1, "interior_camera_health_response_cve": 1},
        }

    def test_process_only_chc_true(self, chc_counter: ChcCounter, only_chc_true):
        # WHEN
        result = chc_counter.process(only_chc_true)

        # THEN
        recording_overview = result["recording_overview"]
        assert recording_overview["number_chc_events"] == 1
        assert recording_overview["chc_duration"] == 20.0

    @fixture
    def chc_not_present_in_between(self, only_chc_true: dict) -> dict:
        only_chc_true[timedelta(seconds=10)] = {}
        return only_chc_true

    def test_process_chc_not_present_in_between(self, chc_counter: ChcCounter, chc_not_present_in_between):
        # WHEN
        result = chc_counter.process(chc_not_present_in_between)

        # THEN
        recording_overview = result["recording_overview"]
        assert recording_overview["number_chc_events"] == 1
        assert recording_overview["chc_duration"] == 20.0

    @fixture
    def chc_not_present_at_all(self) -> dict:
        return {
            timedelta(seconds=5): {},
            timedelta(seconds=10): {},
            timedelta(seconds=15): {},
            timedelta(seconds=20): {}
        }

    def test_process_chc_not_present_at_all(self, chc_counter: ChcCounter, chc_not_present_at_all):
        # WHEN
        result = chc_counter.process(chc_not_present_at_all)

        # THEN
        recording_overview = result["recording_overview"]
        assert recording_overview["number_chc_events"] == 0
        assert recording_overview["chc_duration"] == 0.0
