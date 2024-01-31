"""Rule for audio health check in timeframe"""
import logging

from selector.model import Context
from selector.decision import Decision
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class AudioHealth(BaseRule):
    """
    Rule for request training data
     if there are audio health events in every timeframe
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            attribute_name: str,
            rule_name: str,
            rule_version: str,
            audio_distorted_trigger_per_timeframe: int = 1,
            timeframe_in_frames: int = 60,
            min_ride_length_in_minutes: int = 5,

    ) -> None:
        super().__init__(attribute_name=attribute_name, rule_name=rule_name, rule_version=rule_version)
        self._audio_distorted_trigger_per_timeframe = audio_distorted_trigger_per_timeframe
        self._timeframe_in_frames = timeframe_in_frames
        self._min_ride_length_in_minutes = min_ride_length_in_minutes

    def evaluate(self, context: Context) -> list[Decision]:
        logger.debug("Evaluating '%s' rule", self.rule_name)
        # Get the "interior_camera_health_response_audio_distorted" signal
        audio_distorted_signal = [int(i.value) for i in context.ride_info.preview_metadata.get_integer(
            self.attribute_name) if i.value is not None]

        total_frames = len(audio_distorted_signal)

        if total_frames == 0:
            return []

        # Check if the signal is triggered at least once within every minute
        upload_distorted = True
        for i in range(0, total_frames, self._timeframe_in_frames):
            window = audio_distorted_signal[i:i + self._timeframe_in_frames]
            if sum(window) < self._audio_distorted_trigger_per_timeframe:
                upload_distorted = False
                break

        # Check if the condition is met for the specified duration
        if upload_distorted and total_frames >= self._min_ride_length_in_minutes * 60:
            logger.debug(
                "The Audio health Rule has issued a training upload from %s to %s",
                context.ride_info.start_ride,
                context.ride_info.end_ride,
            )
            return super().evaluate(context=context)
        return []
