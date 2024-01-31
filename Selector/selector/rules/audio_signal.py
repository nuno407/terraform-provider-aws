"""Rule for audio signal """
import logging

from selector.model import Context
from selector.decision import Decision
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class AudioSignal(BaseRule):
    """
    Rule for request training data
    if there was missing one single audio signal
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            attribute_name: str = "interior_camera_health_response_audio_signal",
            rule_name: str = "Audio signal",
            rule_version: str = "1.0.0",
            audio_signal_number_of_trigger: int = 1,
            min_ride_length_in_minutes: int = 5,

    ) -> None:
        super().__init__(attribute_name=attribute_name, rule_name=rule_name, rule_version=rule_version)
        self._audio_signal_number_of_trigger = audio_signal_number_of_trigger
        self._min_ride_length_in_minutes = min_ride_length_in_minutes

    def evaluate(self, context: Context) -> list[Decision]:
        logger.debug("Evaluating '%s' rule", self.rule_name)
        # Get the "interior_camera_health_response_audio_signal" signal
        audio_signal = [int(i.value) for i in context.ride_info.preview_metadata.get_integer(self.attribute_name)
                        if i.value is not None]

        total_frames = len(audio_signal)

        if total_frames == 0:
            return []

        # Check if the signal is triggered once
        upload_signal = False
        for i in range(0, total_frames):
            if audio_signal[i] >= 1:
                upload_signal = True
                break

        # Check if the condition is met for the specified duration
        if upload_signal and total_frames >= self._min_ride_length_in_minutes * 60:
            logger.debug(
                "The Audio signal Rule has issued a training upload from %s to %s",
                context.ride_info.start_ride,
                context.ride_info.end_ride,
            )
            return super().evaluate(context=context)
        return []
