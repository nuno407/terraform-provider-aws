"""Rule Collision event."""
import logging
from typing import Iterator
from typing import List

from base.model.metadata.base_metadata import FrameSignal
from functional import seq  # type: ignore
from selector.model import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class CollisionEvent(BaseRule):
    """Upload training data if a collision event is present in metadata preview."""

    __possible_collision_type_values = {
        'HIGH_IMPACT_COLLISION',
        'LOW_IMPACT_COLLISION'}

    def __init__(
        self, attribute_name: str = "Collision_type",
        rule_name: str = "Collision Detected",
        rule_version: str = "1.0.0"
    ) -> None:
        """
        Args:
            attribute_name (str, optional): Defaults to "Collision_type".
            rule_name (str, optional): Defaults to "Collision Detected".
            rule_version (str, optional): Defaults to "1.0.0"
        """
        super().__init__(attribute_name=attribute_name, rule_name=rule_name, rule_version=rule_version)

    def evaluate(self, context: Context) -> List[Decision]:  # pylint: disable-next=duplicate-code
        """Evaluate if Collision_type is present and determine data to upload.

        Args:
            context (Context): context information accesible to the rule.

        Returns:
            List[Decision]: data to bring into the devcloud.
        """
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if collision event is present during the whole ride
        collision_event_detected = self.check_collision_in_metadata(context.ride_info.preview_metadata)

        # build decision
        if collision_event_detected:
            logger.info(
                "The %s has issued a training upload from %s to %s",
                self.rule_name,
                context.ride_info.start_ride,
                context.ride_info.end_ride,
            )
            return super().evaluate(context=context)
        return []

    def check_collision_in_metadata(self, metadata: PreviewMetadata) -> bool:
        """Logic to select Collision event in metadatapreview file.

        Args:
            metadata (PreviewMetadata): object of metadatapreview

        Returns:
            bool: boolean if collision event
        """
        iterator = metadata.get_string(self.attribute_name)
        return self.is_collision_event_present(iterator=iterator)

    @classmethod
    def is_collision_event_present(cls, iterator: Iterator[FrameSignal]):
        """Logic of filtering events based on present events.

        Args:
            iterator (Iterator[FrameSignal]): Frame iterator result info

        Returns:
            bool: boolean verification if list not empty
        """
        filter_iter = (
            seq(iterator) .filter(
                lambda x: x.value) .map(
                lambda x: x.value in cls.__possible_collision_type_values))

        # check if it exists and all have the same value
        return filter_iter.len() >= 1
