from datetime import datetime, timedelta, timezone
from typing import Any

from pytest import fixture, mark

from base.model.artifacts import RecorderType
from base.model.metadata.base_metadata import IntegerObject
from selector.model import Context, PreviewMetadataV063, RideInfo, Recordings
from selector.rule import Rule
from selector.rules import AudioHealth
from selector.rules.basic_rule import BaseRule

from ..utils import DataTestBuilder


@mark.unit()
class TestAudioBlock:
    @fixture
    def rule(self):
        return AudioHealth("interior_camera_health_response_audio_blocked", "Audio blocked", "1.0.0")

    @fixture
    def data_builder(self) -> DataTestBuilder:
        return DataTestBuilder().with_length(15 * 60).with_frames_per_sec(1)

    @fixture
    def data_audio_true(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_audio_blocked", 1).build()

    @fixture
    def data_audio_false(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_audio_blocked", 0).build()

    @fixture
    def too_short_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            4 * 60).with_integer_attribute("interior_camera_health_response_audio_blocked", 1).build()

    @fixture
    def enough_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            6 * 60).with_integer_attribute("interior_camera_health_response_audio_blocked", 1).build()

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(timezone.utc),
            end_ride=datetime.now(timezone.utc)
        )

    def test_rule_name(self, rule: Rule):
        assert rule.rule_name == "Audio blocked"

    def test_all_audio_selects_ride(
            self,
            rule: Rule,
            data_audio_true: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(data_audio_true),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_no_audio_does_not_select_ride(
            self,
            rule: Rule,
            data_audio_false: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(data_audio_false),
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

    def test_enough_data_select_ride(
            self,
            rule: Rule,
            enough_data: PreviewMetadataV063):
        result = rule.evaluate(
            Context(
                self.ride_info(enough_data),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_some_blocked_audio_but_not_enough_does_not_select_ride(
            self,
            rule: BaseRule,
            data_audio_false: PreviewMetadataV063):

        frame_counter = 0
        first_ignored = False
        for frame in data_audio_false.frames:
            # every 60 frame
            if frame_counter % 60 == 0:
                for object in frame.objectlist:
                    if isinstance(object, IntegerObject):
                        if not first_ignored:
                            first_ignored = True
                        else:
                            object.integer_attributes[rule.attribute_name] = "1"  # type: ignore
            frame_counter += 1

        # WHEN / THEN
        result = rule.evaluate(
            Context(
                self.ride_info(data_audio_false),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_blocked_audio_long_enough_does_select_ride(
            self,
            rule: BaseRule,
            data_audio_false: PreviewMetadataV063):

        frame_counter = 0
        for frame in data_audio_false.frames:
            # every 60 Frame
            if frame_counter % 60 == 0:
                for object in frame.objectlist:
                    if isinstance(object, IntegerObject):
                        object.integer_attributes[rule.attribute_name] = "1"  # type: ignore
            frame_counter += 1

        # WHEN / THEN
        result = rule.evaluate(
            Context(
                self.ride_info(data_audio_false),
                tenant_id="",
                device_id="",
                recordings=Recordings("")))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}
