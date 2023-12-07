from datetime import datetime

from pytest import fixture, mark
from pytz import UTC

from base.model.artifacts import (MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  TimeWindow)
from selector.context import Context
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
    @fixture
    def rule(self):
        return BDDEvent()

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

    @mark.parametrize("event_name", ["Big Damage Detected"])
    def test_rule_name(self, rule: Rule, event_name):
        assert rule.rule_name == event_name

    def test_positive_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BDDEvent,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        context = Context(minimal_preview_metadata, artifact)

        # WHEN
        decisions = rule.evaluate(context)
        # THEN
        recorders = set(map(lambda d: d.recorder, decisions))
        assert recorders == {RecorderType.TRAINING}

    def test_negative_evaluation(self,
                                 minimal_preview_metadata: PreviewMetadataV063,
                                 rule: BDDEvent,
                                 artifact: PreviewSignalsArtifact):
        # GIVEN
        # Mock check_bdd_in_metadata to return a metadata_preview without bdd
        with mock.patch.object(rule, "check_bdd_in_metadata", return_value=False) as check_bdd_in_metadata:
            context = Context(minimal_preview_metadata, artifact)

            # WHEN
            decisions = rule.evaluate(context)

        # assert mocked fucntion was called with input data
        check_bdd_in_metadata.assert_called_once_with(context.preview_metadata)

        # THEN
        recorders = set(map(lambda decision: decision.recorder, decisions))
        assert recorders == set()

    def test_check_bdd_in_metadata(self,
                                   minimal_preview_metadata: PreviewMetadataV063,
                                   rule: BDDEvent,
                                   artifact: PreviewSignalsArtifact):
        # GIVEN
        context = Context(minimal_preview_metadata, artifact)

        # WHEN
        bdd_presence = rule.check_bdd_in_metadata(context.preview_metadata)

        # assert bdd_presence
        assert bdd_presence, "Check BDD presence failed"
