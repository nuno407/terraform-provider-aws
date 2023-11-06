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
from .utils import DataTestBuilder

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
    def data_builder(self) -> DataTestBuilder:
        return DataTestBuilder().with_length(15 * 60).with_frames_per_sec(1)

    @fixture
    def data_person_count_one(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute(self._attribute_name, 1).build()

    @fixture
    def data_person_count_high_variance(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        data = data_builder.with_integer_attribute(self._attribute_name, 1).build()
        pc_value = 0
        for frame in data.frames:
            for object in frame.objectlist:
                if isinstance(object, IntegerObject) and self._attribute_name in object.integer_attributes:
                    object.integer_attributes[self._attribute_name] = str(pc_value)  # type: ignore
                    pc_value += 2
                    if pc_value == 6:
                        pc_value = 0
        return data

    @fixture
    def too_short_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            4 * 60).with_integer_attribute(self._attribute_name, 1).build()

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
                                 data_person_count_high_variance: PreviewMetadataV063,
                                 rule: Rule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        ctx = Context(data_person_count_high_variance, artifact)
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 data_person_count_one: PreviewMetadataV063,
                                 rule: Rule,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        ctx = Context(data_person_count_one, artifact)
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()

    def test_too_short_data_does_not_select_ride(self,
                                                 rule: Rule,
                                                 too_short_data: PreviewMetadataV063,
                                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        ctx = Context(too_short_data, artifact)
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()
