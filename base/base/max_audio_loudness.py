""" Find the maximum audio loudness during a ride. """
import logging
from datetime import timedelta
from typing import Any, Union

from base.processor import Processor

_logger = logging.getLogger("mdfparser." + __name__)


class MaxAudioLoudness(Processor):
    """Processor that calculates the max audio loudness during a ride."""

    @property
    def name(self):
        return "MaxAudioLoudness"

    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]]) -> dict[str, Any]:
        max_audio_loudness = self._calculate_max_audio_loudness(synchronized_signals)
        return {"recording_overview": {
            "max_audio_loudness": max_audio_loudness
        }}

    def _calculate_max_audio_loudness(self,
                                      synchronized_signals: dict[
                                          timedelta, dict[str, Union[bool, int, float]]]) -> float:
        rms_ch0_list = [signals.get("rms_ch0", 0.0) for signals in synchronized_signals.values()]
        rms_ch1_list = [signals.get("rms_ch1", 0.0) for signals in synchronized_signals.values()]

        if not rms_ch0_list and not rms_ch1_list:
            max_audio_loudness = 0.0
        else:
            max_audio_loudness = max(rms_ch0_list + rms_ch1_list)

        _logger.info("Identified %s max Audio loudness", max_audio_loudness)
        return max_audio_loudness
