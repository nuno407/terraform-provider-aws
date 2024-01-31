from datetime import datetime

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow)
from selector.model.context import Context
from selector.model.ride_info import RideInfo
from selector.model import PreviewMetadataV063

from selector.rule import Rule
from selector.rules import BDDEvent
from unittest import mock


tenant_device_and_timing = {
    "artifact_id": "foo",
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": TimeWindow(start=datetime.now(tz=UTC), end=datetime.now(tz=UTC))
}


@mark.unit()
class TestBDDEventRule:

    def ride_info(self, artifact: PreviewMetadataV063) -> RideInfo:
        return RideInfo(
            preview_metadata=artifact,
            start_ride=datetime.now(tz=UTC),
            end_ride=datetime.now(tz=UTC)
        )

    @fixture
    def rule(self):
        return BDDEvent()

    @mark.parametrize("event_name", ["Big Damage Detected"])
    def test_rule_name(self, rule: Rule, event_name):
        assert rule.rule_name == event_name

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BDDEvent):
        # GIVEN
        context = Context(self.ride_info(minimal_preview_metadata), tenant_id="", device_id="")

        # WHEN
        decisions = rule.evaluate(context)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BDDEvent):
        # GIVEN
        # Mock check_bdd_in_metadata to return a metadata_preview without bdd
        with mock.patch.object(rule, "check_bdd_in_metadata", return_value=False) as check_bdd_in_metadata:
            context = Context(self.ride_info(minimal_preview_metadata), tenant_id="", device_id="")

            # WHEN
            decisions = rule.evaluate(context)

        # assert mocked fucntion was called with input data
        check_bdd_in_metadata.assert_called_once_with(context.ride_info.preview_metadata)

        # THEN
        recorders = set(map(lambda decision: decision.recorder, decisions))
        assert recorders == set()

    def test_check_bdd_in_metadata(self,
                                   minimal_preview_metadata: PreviewMetadataV063,
                                   rule: BDDEvent):
        # GIVEN
        context = context = Context(self.ride_info(minimal_preview_metadata), tenant_id="", device_id="")

        # WHEN
        bdd_presence = rule.check_bdd_in_metadata(context.ride_info.preview_metadata)

        # assert bdd_presence
        assert bdd_presence, "Check BDD presence failed"
