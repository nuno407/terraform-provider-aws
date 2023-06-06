"""Rule for camera always blocked"""
from functional import seq  # type: ignore

from base.model.artifacts import RecorderType
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rule import Rule


class CameraAlwaysBlockedRule(Rule):
    """Rule that uploads training data if interior_camera_health_response_cvb is always bigger then 1.0"""
    @property
    def _attribute_name(self) -> str:
        return "interior_camera_health_response_cvb"

    @property
    def rule_name(self) -> str:
        return "Camera completely blocked"

    def evaluate(self, context: Context) -> list[Decision]:
        # check if camera view is blocked during the whole ride
        view_always_blocked = self._is_view_always_blocked(context.preview_metadata)

        # build decision
        if view_always_blocked:
            return [Decision(recorder=RecorderType.TRAINING,
                             footage_from=context.metadata_artifact.timestamp,
                             footage_to=context.metadata_artifact.end_timestamp)]
        return []

    def _is_view_always_blocked(self, metadata: PreviewMetadata) -> bool:
        filter_iter = seq(metadata.get_integer(self._attribute_name)) \
            .map(lambda x: x.value) \
            .filter(lambda x: x is not None) \
            .map(lambda x: x >= 1)

        return filter_iter.len() >= 1 and filter_iter.all()
