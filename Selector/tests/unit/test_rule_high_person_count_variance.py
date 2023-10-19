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
from selector.rules import HighPersonCountVarianceRule

tenant_device_and_timing = {
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))
}


@mark.unit()
class TestRuleHighPersonCount:
    @property
    def _attribute_name(self) -> str:
        return "PersonCount_value"

    @fixture
    def rule(self):
        return HighPersonCountVarianceRule()

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
        assert rule.rule_name == "High Person Count Variance"

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: Rule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        pc_value = 0
        for frame in minimal_preview_metadata.frames:
            for object in frame.objectlist:
                if isinstance(
                        object, IntegerObject) and self._attribute_name in object.integer_attributes:
                    object.integer_attributes[self._attribute_name] = str(pc_value)  # type: ignore
                    pc_value += 1
        ctx = Context(minimal_preview_metadata, artifact)

        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: Rule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        for i, frame in enumerate(minimal_preview_metadata.frames):
            for object in frame.objectlist:
                if isinstance(object, IntegerObject):
                    object.integer_attributes[self._attribute_name] = "1"  # type: ignore
        ctx = Context(minimal_preview_metadata, artifact)

        # WHEN
        decisions = rule.evaluate(ctx)

        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()
