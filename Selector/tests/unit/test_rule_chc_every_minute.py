from datetime import datetime, timedelta

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow,)
from selector.context import Context
from selector.model.preview_metadata import FloatObject
from selector.model.preview_metadata_63 import PreviewMetadataV063
from selector.rule import Rule
from selector.rules import CHCEveryMinute

from .utils import DataTestBuilder

tenant_device_and_timing = {
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))
}


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
        return data_builder.with_float_attribute("interior_camera_health_response_cve", 1.1).build()

    @fixture
    def data_chc_false(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_float_attribute("interior_camera_health_response_cve", 0.4).build()

    @fixture
    def too_short_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            5 * 60).with_float_attribute("interior_camera_health_response_cve", 0.4).build()

    @fixture
    def artifact(self) -> PreviewSignalsArtifact:
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
        assert rule.rule_name == "CHC event every minute"

    def test_all_chc_selects_ride(
            self,
            rule: Rule,
            data_chc_true: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(data_chc_true, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_no_chc_does_not_select_ride(
            self,
            rule: Rule,
            data_chc_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(data_chc_false, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_too_short_data_does_not_select_ride(
            self,
            rule: Rule,
            too_short_data: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(too_short_data, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_chc_but_too_short_does_not_select_ride(
            self,
            rule: Rule,
            data_chc_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        # GIVEN
        first_ts = data_chc_false.footage_from
        last_ts = first_ts + timedelta(minutes=8)
        for frame in data_chc_false.frames:
            ts = data_chc_false.get_frame_utc_timestamp(frame)
            for object in frame.objectlist:
                if isinstance(object, FloatObject) and ts < last_ts:
                    object.float_attributes[0]["interior_camera_health_response_cve"] = 1.1

        # WHEN / THEN
        result = rule.evaluate(Context(data_chc_false, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_chc_long_enough_does_select_ride(
            self,
            rule: Rule,
            data_chc_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):

        # GIVEN
        first_ts = data_chc_false.footage_from
        last_ts = first_ts + timedelta(minutes=11)
        for frame in data_chc_false.frames:
            for object in frame.objectlist:
                ts = data_chc_false.get_frame_utc_timestamp(frame)
                if isinstance(object, FloatObject) and ts < last_ts:
                    object.float_attributes[0]["interior_camera_health_response_cvb"] = 1.0

        # WHEN / THEN
        result = rule.evaluate(Context(data_chc_false, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}
