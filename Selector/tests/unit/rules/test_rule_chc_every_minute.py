from datetime import datetime, timedelta

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import RecorderType
from base.model.metadata.base_metadata import IntegerObject
from selector.model import Context, RideInfo, PreviewMetadataV063, Recordings
from selector.rule import Rule
from selector.rules import CHCEveryMinute
from selector.rules.basic_rule import BaseRule

from ..utils import DataTestBuilder


@mark.unit()
class TestChcEveryMinute:
    @fixture
    def rule(self):
        return CHCEveryMinute()

    @fixture
    def data_builder(self) -> DataTestBuilder:
        return DataTestBuilder().with_length(15 * 60).with_frames_per_sec(1)

    @fixture
    def data_chc_true(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_cve", 2).build()

    @fixture
    def data_chc_false(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_cve", 0).build()

    @fixture
    def too_short_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            5 * 60).with_integer_attribute("interior_camera_health_response_cve", 0).build()

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(tz=UTC),
            end_ride=datetime.now(tz=UTC)
        )

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "CHC event every minute"

    def test_all_chc_selects_ride(
            self,
            rule: Rule,
            data_chc_true: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(data_chc_true),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_no_chc_does_not_select_ride(
            self,
            rule: Rule,
            data_chc_false: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(data_chc_false),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_too_short_data_does_not_select_ride(
            self,
            rule: Rule,
            too_short_data: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(too_short_data),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_chc_but_too_short_does_not_select_ride(
            self,
            rule: BaseRule,
            data_chc_false: PreviewMetadataV063):
        # GIVEN
        first_ts = data_chc_false.footage_from
        last_ts = first_ts + timedelta(minutes=8)
        for frame in data_chc_false.frames:
            ts = data_chc_false.get_frame_utc_timestamp(frame)
            for object in frame.objectlist:
                if isinstance(object, IntegerObject) and ts < last_ts:
                    object.integer_attributes[rule.attribute_name] = 1  # type: ignore

        # WHEN / THEN
        result = rule.evaluate(
            Context(
                self.ride_info(data_chc_false),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_chc_long_enough_does_select_ride(
            self,
            rule: BaseRule,
            data_chc_false: PreviewMetadataV063):

        # GIVEN
        first_ts = data_chc_false.footage_from
        last_ts = first_ts + timedelta(minutes=11)
        for frame in data_chc_false.frames:
            for object in frame.objectlist:
                ts = data_chc_false.get_frame_utc_timestamp(frame)
                if isinstance(object, IntegerObject) and ts < last_ts:
                    object.integer_attributes[rule.attribute_name] = 1  # type: ignore

        # WHEN / THEN
        result = rule.evaluate(
            Context(
                self.ride_info(data_chc_false),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}
