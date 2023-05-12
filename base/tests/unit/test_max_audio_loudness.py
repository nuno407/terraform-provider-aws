""" Module that tests mean audio bias Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.max_audio_loudness import MaxAudioLoudness


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestMedianPersonCounter:
    @fixture
    def max_audio_loudness(self):
        return MaxAudioLoudness()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def loud_right_signals(self) -> dict:
        return {
            timedelta(seconds=5): {"rms_ch0": -30, "rms_ch1": -20},
            timedelta(seconds=15): {"rms_ch0": -30, "rms_ch1": -40},
            timedelta(seconds=16): {"rms_ch0": -30, "rms_ch1": -60},
            timedelta(seconds=20): {"rms_ch0": -30, "rms_ch1": -80}
        }

    @fixture
    def loud_left_signals(self) -> dict:
        return {
            timedelta(seconds=5): {"rms_ch0": -60, "rms_ch1": -30},
            timedelta(seconds=15): {"rms_ch0": -40, "rms_ch1": -30},
            timedelta(seconds=16): {"rms_ch0": -20, "rms_ch1": -30},
            timedelta(seconds=20): {"rms_ch0": -10, "rms_ch1": -30}
        }

    def test_empty_signal_max_audio_loudness(self, max_audio_loudness: MaxAudioLoudness, empty_signal):
        # WHEN
        max_audio_loudness_value = max_audio_loudness.process(
            # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert max_audio_loudness_value["recording_overview"]["max_audio_loudness"] == 0

    def test_loud_right_signals_max_audio_loudness(self, max_audio_loudness: MaxAudioLoudness, loud_right_signals):
        # WHEN
        max_audio_loudness_value = max_audio_loudness.process(
            # type: ignore # pylint: disable=protected-access
            loud_right_signals)

        # THEN
        assert max_audio_loudness_value["recording_overview"]["max_audio_loudness"] == -20

    def test_loud_left_signals_max_audio_loudness(self, max_audio_loudness: MaxAudioLoudness, loud_left_signals):
        # WHEN
        max_audio_loudness_value = max_audio_loudness.process(
            # type: ignore # pylint: disable=protected-access
            loud_left_signals)

        # THEN
        assert max_audio_loudness_value["recording_overview"]["max_audio_loudness"] == -10
