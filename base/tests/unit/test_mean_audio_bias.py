""" Module that tests mean audio bias Processor. """
from datetime import timedelta

import pytest
from pytest import fixture

from base.mean_audio_bias import MeanAudioBias


@pytest.mark.unit
# pylint: disable=missing-function-docstring,missing-class-docstring,duplicate-code
class TestMedianPersonCounter:
    @fixture
    def mean_audio_bias(self):
        return MeanAudioBias()

    @fixture
    def empty_signal(self) -> dict:
        return {
        }

    @fixture
    def simple_signals(self) -> dict:
        return {
            timedelta(seconds=5): {"sum_ch0": 0.3, "sum_ch1": 0.2},
            timedelta(seconds=15): {"sum_ch0": 0.3, "sum_ch1": 0.4},
            timedelta(seconds=16): {"sum_ch0": 0.3, "sum_ch1": 0.6},
            timedelta(seconds=20): {"sum_ch0": 0.3, "sum_ch1": 0.8}
        }

    def test_empty_signal_mean_audio_bias(self, mean_audio_bias: MeanAudioBias, empty_signal):
        # WHEN
        mean_audio_bias_value = mean_audio_bias.process(  # type: ignore # pylint: disable=protected-access
            empty_signal)

        # THEN
        assert mean_audio_bias_value["recording_overview"]["mean_audio_bias"] == 0

    def test_simple_signal_mean_audio_bias(self, mean_audio_bias: MeanAudioBias, simple_signals):
        # WHEN
        mean_audio_bias_value = mean_audio_bias.process(  # type: ignore # pylint: disable=protected-access
            simple_signals)

        # THEN
        assert mean_audio_bias_value["recording_overview"]["mean_audio_bias"] == 0.4
