from datetime import datetime

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import RecorderType
from base.model.metadata.base_metadata import IntegerObject
from selector.model import Context, RideInfo, PreviewMetadataV063
from selector.rule import Rule
from selector.rules import HighPersonCountVarianceRule
from ..utils import DataTestBuilder


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

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(tz=UTC),
            end_ride=datetime.now(tz=UTC)
        )

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "High Person Count Variance"

    def test_positive_evaluation(self,
                                 data_person_count_high_variance: PreviewMetadataV063,
                                 rule: Rule):
        # GIVEN
        ctx = Context(self.ride_info(data_person_count_high_variance), tenant_id="", device_id="")
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 data_person_count_one: PreviewMetadataV063,
                                 rule: Rule):
        # GIVEN
        ctx = Context(self.ride_info(data_person_count_one), tenant_id="", device_id="")
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()

    def test_too_short_data_does_not_select_ride(self,
                                                 rule: Rule,
                                                 too_short_data: PreviewMetadataV063):
        # GIVEN
        ctx = Context(self.ride_info(too_short_data), tenant_id="", device_id="")
        # WHEN
        decisions = rule.evaluate(ctx)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == set()
