"""Rule for device ingest until."""
from datetime import datetime, timedelta, timezone
import logging

from selector.model import Context
from selector.decision import Decision
from selector.model import Recordings, RecordingOptions
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class DeviceIngestUntil(BaseRule):
    """Rule that always allows ingestion of recordings up to a certain amount per 24h per device.
       This rule acts as an example on how to use the Recordings interface within the Context"""

    def __init__(self,
                 attribute_name: str = "",
                 rule_name: str = "Device ingest until",
                 rule_version: str = "1.0.0",
                 limit_per_day: int = 3) -> None:
        super().__init__(attribute_name, rule_name, rule_version)
        self._limit_per_day = limit_per_day

    def evaluate(self, context: Context) -> list[Decision]:
        """ Evaluate if there are recordings to be ingested for a device,
            as long as the limit of recordings per day is not exceeded
        Args:
            context (Context): Object containing all values and data sources for a Rule to operate its logic
        Returns:
            Decision: Request training recorders if the condition matches.
        """
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if the videos ingested for this device in the last 24 hours exceed the limit
        ingestion_exceeded = self.__assess_daily_ingestion(context.device_id, context.recordings)

        # create decision based on ingestion assessment
        if not ingestion_exceeded:
            logger.debug(
                "The %s rule has issued a training upload from %s to %s", self.rule_name,
                context.ride_info.start_ride, context.ride_info.end_ride
            )
            return super().evaluate(context=context)
        return []

    def __assess_daily_ingestion(self, device_id: str, recordings: Recordings) -> bool:
        """ Check if the videos ingested for this device in the last 24 hours exceed the limit
        Args:
            device_id (str): device identifier
            recordings (Recordings): Recordings object
        Returns:
            bool: True if the limit is exceeded, False otherwise
        """
        return recordings.count_videos(
            RecordingOptions(
                device_id=device_id,
                from_timestamp=datetime.now(
                    timezone.utc) - timedelta(days=1))) >= self._limit_per_day
