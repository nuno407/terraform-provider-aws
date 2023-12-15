from datetime import datetime, timedelta

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow,)
from base.model.metadata.base_metadata import IntegerObject
from selector.context import Context
from selector.model.preview_metadata_63 import PreviewMetadataV063
from selector.rule import Rule
from selector.rules import AudioHealth
from selector.rules.basic_rule import BaseRule

from .utils import DataTestBuilder

tenant_device_and_timing = {
    "artifact_id": "foo",
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))
}


@mark.unit()
class TestAudioBlock:
    @fixture
    def rule(self):
        return AudioHealth("interior_camera_health_response_audio_distorted", "Audio distorted", "1.0.0")

    @fixture
    def data_builder(self) -> DataTestBuilder:
        return DataTestBuilder().with_length(15 * 60).with_frames_per_sec(1)

    @fixture
    def data_audio_true(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_audio_distorted", 1).build()

    @fixture
    def data_audio_false(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_integer_attribute("interior_camera_health_response_audio_distorted", 0).build()

    @fixture
    def too_short_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            4 * 60).with_integer_attribute("interior_camera_health_response_audio_distorted", 1).build()

    @fixture
    def enough_data(self, data_builder: DataTestBuilder) -> PreviewMetadataV063:
        return data_builder.with_length(
            6 * 60).with_integer_attribute("interior_camera_health_response_audio_distorted", 1).build()

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
        assert rule.rule_name == "Audio distorted"

    def test_all_audio_selects_ride(
            self,
            rule: Rule,
            data_audio_true: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(data_audio_true, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_no_audio_does_not_select_ride(
            self,
            rule: Rule,
            data_audio_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(data_audio_false, artifact))
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

    def test_enough_data_select_ride(
            self,
            rule: Rule,
            enough_data: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):
        result = rule.evaluate(Context(enough_data, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}

    def test_some_distorted_audio_but_not_enough_does_not_select_ride(
            self,
            rule: BaseRule,
            data_audio_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):

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
        result = rule.evaluate(Context(data_audio_false, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == set()

    def test_some_distorted_audio_long_enough_does_select_ride(
            self,
            rule: BaseRule,
            data_audio_false: PreviewMetadataV063,
            artifact: PreviewSignalsArtifact):

        frame_counter = 0
        for frame in data_audio_false.frames:
            # every 60 Frame
            if frame_counter % 60 == 0:
                for object in frame.objectlist:
                    if isinstance(object, IntegerObject):
                        object.integer_attributes[rule.attribute_name] = "1"  # type: ignore
            frame_counter += 1

        # WHEN / THEN
        result = rule.evaluate(Context(data_audio_false, artifact))
        recorders = set(map(lambda d: d.recorder, result))
        assert recorders == {RecorderType.TRAINING}
