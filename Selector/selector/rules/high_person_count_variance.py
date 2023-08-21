"""Rule for high person count variance"""
import logging

from base.model.artifacts import RecorderType
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rule import Rule

logger = logging.getLogger(__name__)


class HighPersonCountVarianceRule(Rule):
    """Rule that uploads training data if PersonCount_value variance is bigger than 1"""
    @property
    def attribute_name(self) -> str:
        """attribute name"""
        return "PersonCount_value"

    @property
    def rule_name(self) -> str:
        """rule name"""
        return "High Person Count Variance"

    def evaluate(self, context: Context) -> list[Decision]:
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if the person count variance is high during the whole ride
        high_person_count_variance_detected = self._has_high_person_count_variance(context.preview_metadata)

        # build decision
        if high_person_count_variance_detected:
            logger.info(
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
        if len(person_count) == 0:
            variance = float(0)
        else:
            mean = sum(person_count) / len(person_count)
            variance = sum((xi - mean) ** 2 for xi in person_count) / len(person_count)
            variance = round(variance, 2)
        return variance > 1
