"""Rule BDD event."""
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


class BDDEvent(BaseRule):
    """Rule that uploads training data if big damage event is present in metadata preview."""

    def __init__(
        self, attribute_name: str = "BigDamage_detected",
        rule_name: str = "Big Damage Detected",
        rule_version: str = "1.0.0"
    ) -> None:
        """_summary_

        Args:
            attribute_name (str, optional): _description_. Defaults to "BigDamage_detected".
            rule_name (str, optional): _description_. Defaults to "Big Damage Detected".
        """
        super().__init__(attribute_name=attribute_name, rule_name=rule_name, rule_version=rule_version)

    def evaluate(self, context: Context) -> List[Decision]:  # pylint: disable-next=duplicate-code
        """_summary_

        Args:
            context (Context): _description_

        Returns:
            List[Decision]: _description_
        """
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if big damage event is present during the whole ride
        bdd_event_detected = self.check_bdd_in_metadata(context.ride_info.preview_metadata)

        # build decision
        if bdd_event_detected:
            logger.info(
                "The %s has issued a training upload from %s to %s",
                self.rule_name,
                context.ride_info.start_ride,
                context.ride_info.end_ride,
            )
            return super().evaluate(context=context)
        return []

    def check_bdd_in_metadata(self, metadata: PreviewMetadata) -> bool:
        """Logic to select BDD event in metadatapreview file.

        Args:
            metadata (PreviewMetadata): object of metadatapreview

        Returns:
            bool: boolean if bdd event
        """
        iterator = metadata.get_bool(self.attribute_name)
        return self.is_bdd_event_present(iterator=iterator)

    @staticmethod
    def is_bdd_event_present(iterator: Iterator[FrameSignal]):
        """Logic of filtering events based on present events.

        Args:
            iterator (Iterator[FrameSignal]): Frame iterator result info

        Returns:
            bool: boolean verification if list not empty
        """
        filter_iter = (seq(iterator)
                       .filter(lambda x: x.value)
                       .map(lambda x: x.value >= 1))

        # check if it exists and all have the same value
        return filter_iter.len() >= 1
