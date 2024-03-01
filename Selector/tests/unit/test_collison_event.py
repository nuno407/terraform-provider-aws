from datetime import datetime, timezone

from pytest import fixture, mark

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow)
from selector.model.context import Context
from selector.model.ride_info import RideInfo
from selector.model import PreviewMetadataV063, Recordings

from selector.rule import Rule
from selector.rules import CollisionEvent
from unittest import mock


tenant_device_and_timing = {
    "artifact_id": "foo",
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(timezone.utc),
    "end_timestamp": datetime.now(timezone.utc),
    "upload_timing": TimeWindow(start=datetime.now(timezone.utc), end=datetime.now(timezone.utc))
}


@mark.unit()
class TestCollisionEventRule:

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(timezone.utc),
            end_ride=datetime.now(timezone.utc)
        )

    @fixture
    def rule(self):
        return CollisionEvent()

    @mark.parametrize("event_name", ["Collision Detected"])
    def test_rule_name(self, rule: Rule, event_name):
        assert rule.rule_name == event_name

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: CollisionEvent):
        # GIVEN
        context = Context(
            self.ride_info(minimal_preview_metadata),
            tenant_id="",
            device_id="",
            recordings=Recordings(""))

        # WHEN
        decisions = rule.evaluate(context)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: CollisionEvent):
        # GIVEN
        # Mock check_collision_in_metadata to return a metadata_preview without collision
        with mock.patch.object(rule, "check_collision_in_metadata", return_value=False) as check_collision_in_metadata:
            context = Context(
                self.ride_info(minimal_preview_metadata),
                tenant_id="",
                device_id="",
                recordings=Recordings(""))

            # WHEN
            decisions = rule.evaluate(context)

        # assert mocked fucntion was called with input data
        check_collision_in_metadata.assert_called_once_with(context.ride_info.preview_metadata)

        # THEN
        recorders = set(map(lambda decision: decision.recorder, decisions))
        assert recorders == set()

    def test_check_collision_in_metadata(self,
                                         minimal_preview_metadata: PreviewMetadataV063,
                                         rule: CollisionEvent):
        # GIVEN
        context = Context(
            self.ride_info(minimal_preview_metadata),
            tenant_id="",
            device_id="",
            recordings=Recordings(""))

        # WHEN
        collision_presence = rule.check_collision_in_metadata(context.ride_info.preview_metadata)

        # assert collision_presence
        assert collision_presence, "Check Collision presence failed"
