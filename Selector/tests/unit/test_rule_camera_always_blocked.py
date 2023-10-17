from datetime import datetime

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow)
from base.model.metadata.base_metadata import IntegerObject
from selector.context import Context
from selector.model import PreviewMetadataV063
from selector.rule import Rule
from selector.rules import CameraAlwaysBlockedRule
from selector.rules.basic_rule import BaseRule

tenant_device_and_timing = {
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))
}


@mark.unit()
class TestRuleCameraAlwaysBlocked:

    @fixture
    def rule(self):
        return CameraAlwaysBlockedRule()

    @fixture
    def artifact(self):
        return PreviewSignalsArtifact(
            **tenant_device_and_timing,
            referred_artifact=MultiSnapshotArtifact(
                **tenant_device_and_timing,
                recorder=RecorderType.INTERIOR_PREVIEW,
                chunks=[],
                recording_id="foo"
            )
        )

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "Camera completely blocked"

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BaseRule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        for frame in minimal_preview_metadata.frames:
            for object in frame.objectlist:
                if isinstance(
                        object, IntegerObject) and rule.attribute_name in object.integer_attributes[0]:
                    object.integer_attributes[0][rule.attribute_name] = "1"  # type: ignore
        ctx = Context(minimal_preview_metadata, artifact)

        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BaseRule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        for i, frame in enumerate(minimal_preview_metadata.frames):
            for object in frame.objectlist:
                if isinstance(object, IntegerObject):
                    object.integer_attributes[0][rule.attribute_name] = "1" if i > 2 else "0"  # type: ignore
        ctx = Context(minimal_preview_metadata, artifact)

        # WHEN
        decisions = rule.evaluate(ctx)

        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()
