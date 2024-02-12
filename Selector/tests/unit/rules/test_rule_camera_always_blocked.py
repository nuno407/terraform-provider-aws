from datetime import datetime

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import RecorderType
from base.model.metadata.base_metadata import IntegerObject
from selector.model import Context, RideInfo, PreviewMetadataV063, Recordings
from selector.rule import Rule
from selector.rules import CameraAlwaysBlockedRule
from selector.rules.basic_rule import BaseRule


@mark.unit()
class TestRuleCameraAlwaysBlocked:

    @fixture
    def rule(self):
        return CameraAlwaysBlockedRule()

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(tz=UTC),
            end_ride=datetime.now(tz=UTC)
        )

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "Camera completely blocked"

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BaseRule):
        # GIVEN
        for frame in minimal_preview_metadata.frames:
            for object in frame.objectlist:
                if isinstance(
                        object, IntegerObject) and rule.attribute_name in object.integer_attributes:
                    object.integer_attributes[rule.attribute_name] = 1  # type: ignore
        ctx = Context(self.ride_info(minimal_preview_metadata), tenant_id="", device_id="", recordings=Recordings(""))

        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BaseRule):
        # GIVEN
        for i, frame in enumerate(minimal_preview_metadata.frames):
            for object in frame.objectlist:
                if isinstance(object, IntegerObject):
                    object.integer_attributes[rule.attribute_name] = 1 if i > 2 else 0  # type: ignore
        ctx = Context(self.ride_info(minimal_preview_metadata), tenant_id="", device_id="", recordings=Recordings(""))

        # WHEN
        decisions = rule.evaluate(ctx)

        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()
