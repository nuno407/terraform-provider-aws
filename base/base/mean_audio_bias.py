""" Sums up the audio bias and calculates the mean during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class MeanAudioBias(Processor):
    """Processor that calculates the mean of the audio bias during a ride."""

    @property
    def name(self):
        return "MeanAudioBias"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        mean_audio_bias = self._calculate_mean_audio_bias(synchronized_signals)
        return {"recording_overview": {
            "mean_audio_bias": mean_audio_bias
        }}

    def _calculate_mean_audio_bias(self,
                                   synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> float:
        sum_ch0_list = [signals.get("sum_ch0", 0.0) for signals in synchronized_signals.values()]
        sum_ch1_list = [signals.get("sum_ch1", 0.0) for signals in synchronized_signals.values()]

        if len(sum_ch0_list) > 0 and len(sum_ch1_list) > 0:
            mean_audio_bias = (sum(sum_ch0_list) + sum(sum_ch1_list)) / (len(sum_ch0_list) + len(sum_ch1_list))
        else:
            mean_audio_bias = 0.0

        _logger.info("Identified %s mean Audio bias", mean_audio_bias)
        return mean_audio_bias
