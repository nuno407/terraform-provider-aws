"""Rule for chc every minute"""

import logging
from datetime import datetime, timedelta
from math import ceil
from typing import cast
from functional import seq  # type: ignore

from base.model.artifacts import RecorderType
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.model.preview_metadata import FrameSignal
from selector.rule import Rule

logger = logging.getLogger(__name__)


class CHCEveryMinute(Rule):
    """
    Rule for request training data
    if there are constant CHC events for a certain period of time
    """

    def __init__(
            self,
            window_size_seconds: int = 60,
            min_hits_per_window: int = 1,
            min_consecutive_window_hits: int = 10) -> None:
        super().__init__()
        self._window_size_seconds = window_size_seconds
        self._min_hits_per_window = min_hits_per_window
        self._min_consecutive_window_hits = min_consecutive_window_hits

    @property
    def rule_name(self) -> str:
        """The rule name"""
        return "CHC event every minute"

    def evaluate(self, context: Context) -> list[Decision]:
        """
        Evalutes if there is a time range of ten minutes where we have at least one chc event per minute.

        Args:
            context (Context): The context

        Returns:
            Decision: Request training recorders if the condition matches.
        """
        logger.debug("Evaluating '%s' rule", self.rule_name)
        # do not request anything when Metadata version is not supported
        chc_per_window: list[int] = self.__get_chc_per_window(context.preview_metadata)

        # Count events using a sliding window, returns true if every minute
        # contains at least one chc event
        for window_end in range(self._min_consecutive_window_hits, len(chc_per_window)):
            window_start = window_end - self._min_consecutive_window_hits
            all_chc = all(map(lambda x: x >= self._min_hits_per_window,
                          chc_per_window[window_start:window_end]))
            if all_chc:
                logger.info(
                    "The %s has issued a training upload from %s to %s",
                    self.rule_name,
                    context.metadata_artifact.timestamp,
                    context.metadata_artifact.end_timestamp)
                return [Decision(recorder=RecorderType.TRAINING,
                                 footage_from=context.metadata_artifact.timestamp,
                                 footage_to=context.metadata_artifact.end_timestamp)]
        return []

    @staticmethod
    def __get_any_blocked(blocked: FrameSignal, shifted: FrameSignal) -> bool:
        """
        Returns a boolean for any signal chc event

        Args:
            blocked (FrameSignal): Blocked signal
            shifted (FrameSignal): Shifted signal

        Returns:
            bool: True if any there is a chc event, return false otherwise
        """
        if blocked.value is not None and cast(float, blocked.value) >= 1:
            return True
        if shifted.value is not None and cast(float, shifted.value) >= 1:
            return True
        return False

    def __get_chc_per_window(self, metadata: PreviewMetadata) -> list[int]:
        """
        Returns a dictionary containing the number of chc events per time window.

        Args:
            metadata (PreviewMetadata): The PreviewMetadata to search in

        Returns:
            list[int]: -
        """
        sorted_chc: list[tuple[datetime, bool]] = seq(metadata.get_integer("interior_camera_health_response_cvb")) \
            .zip(metadata.get_integer("interior_camera_health_response_cve")) \
            .map(lambda x: (x[0].utc_time, self.__get_any_blocked(x[0], x[1]))) \
            .sorted(key=lambda x: x[0]) \
            .to_list()

        # Skip if metadata doesn't have frames
        if len(sorted_chc) == 0:
            return []

        all_frames_duration: float = (
            (sorted_chc[-1][0] - sorted_chc[0][0]).total_seconds())
        number_of_windows: int = ceil(all_frames_duration / self._window_size_seconds)

        chc_per_window = [self.__get_number_of_chc_in_window(
            sorted_chc, window) for window in range(0, number_of_windows)]

        return chc_per_window

    def __get_number_of_chc_in_window(
            self, sorted_chc: list[tuple[datetime, bool]], window_number: int) -> int:
        """
        Return the list of frames that exists in a time window.
        The actual time window is calculated based on the window number and the window size.

        Args:
            frames (list[Frame]): The list of frames to search on.
            window_number (int): The window number to search for.

        Returns:
            list[Frame]: The list of frames within that timestamp in the same order.
        """

        time_start = sorted_chc[0][0] + timedelta(seconds=window_number * self._window_size_seconds)
        time_end = time_start + timedelta(seconds=self._window_size_seconds)

        return seq(sorted_chc) \
            .filter(lambda x: x[0] >= time_start and x[0] < time_end) \
            .map(lambda x: x[1]) \
            .filter(lambda x: x is not None) \
            .sum()
