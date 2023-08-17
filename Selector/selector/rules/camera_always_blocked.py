"""Rule for camera always blocked"""
import logging

from functional import seq  # type: ignore
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rules.basic_rule import BaseRule

logger = logging.getLogger(__name__)


class CameraAlwaysBlockedRule(BaseRule):
    """Rule that uploads training data if interior_camera_health_response_cvb is always bigger then 1.0"""

    def __init__(self,
                 attribute_name: str = "interior_camera_health_response_cvb",
                 rule_name: str = "Camera completely blocked") -> None:
        super().__init__(attribute_name, rule_name)

    def evaluate(self, context: Context) -> list[Decision]:
        logger.debug("Evaluating '%s' rule", self.rule_name)

        # check if camera view is blocked during the whole ride
        view_always_blocked = self._is_view_always_blocked(context.preview_metadata)

        # build decision
        if view_always_blocked:
            logger.info(
                "The %s has issued a training upload from %s to %s", self.rule_name,
                context.metadata_artifact.timestamp, context.metadata_artifact.end_timestamp,
            )
            return super().evaluate(context=context)
        return []

    def _is_view_always_blocked(self, metadata: PreviewMetadata) -> bool:
        filter_iter = seq(metadata.get_integer(self.attribute_name)) \
            .map(lambda x: x.value) \
            .filter(lambda x: x is not None) \
            .map(lambda x: x >= 1)

        return filter_iter.len() >= 1 and filter_iter.all()
