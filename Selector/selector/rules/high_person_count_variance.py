"""Rule for high person count variance"""
import logging

from base.model.artifacts import RecorderType
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class HighPersonCountVarianceRule(BaseRule):
    """Rule that uploads training data if PersonCount_value variance is bigger than 1.5"""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            attribute_name: str = "PersonCount_value",
            rule_name: str = "High Person Count Variance",
            min_ride_length_in_minutes: int = 1,

    ) -> None:
        super().__init__(attribute_name=attribute_name, rule_name=rule_name)
        self._min_ride_length_in_minutes = min_ride_length_in_minutes

    def evaluate(self, context: Context) -> list[Decision]:
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if the person count variance is high during the whole ride
        high_person_count_variance_detected = self._has_high_person_count_variance(context.preview_metadata)

        # build decision
        if high_person_count_variance_detected:
            logger.debug(
                "The %s has issued a training upload starting at %s to %s",
                self.rule_name,
                context.metadata_artifact.timestamp,
                context.metadata_artifact.end_timestamp
            )
            return [Decision(recorder=RecorderType.TRAINING,
                             footage_from=context.metadata_artifact.timestamp,
                             footage_to=context.metadata_artifact.end_timestamp)]
        return []

    def _has_high_person_count_variance(self, metadata: PreviewMetadata) -> bool:
        person_count = [int(i.value) for i in metadata.get_integer(self.attribute_name) if i.value is not None]
        if len(person_count) <= self._min_ride_length_in_minutes * 60:
            variance = float(0)
        else:
            mean = sum(person_count) / len(person_count)
            variance = sum((xi - mean) ** 2 for xi in person_count) / len(person_count)
            variance = round(variance, 2)
        return variance > 1.5
