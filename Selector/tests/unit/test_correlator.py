""" Correlator Tests. """
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, call

import pytest

from base.model.artifacts import (DEFAULT_RULE, DEFAULT_RULE_NAME, DEFAULT_RULE_VERSION,
                                  RecorderType, Recording, RuleOrigin, S3VideoArtifact, SelectorRule,
                                  SnapshotArtifact, SnapshotUploadRule, TimeWindow, VideoUploadRule)
from selector.constants import CONTAINER_NAME
from selector.correlator import Correlator
from selector.model import DBDecision


@pytest.mark.unit
class TestCorrelator():
    @pytest.mark.parametrize("rule_parameters", [
        ([
            {
                "rule_name": "Test Rule",
                "rule_version": "1.0.0",
                "origin": RuleOrigin.INTERIOR
            },
            {
                "rule_name": "Another Rule",
                "rule_version": "2.0.0",
                "origin": RuleOrigin.INTERIOR
            }
        ]),
        (
            []
        )
    ])
    def test_correlate_video_rules(self, rule_parameters: list[dict]):
        # GIVEN
        sqs_controller = Mock()
        sqs_controller.send_message = Mock(return_value=None)

        cont_services = MagicMock()
        cont_services.sqs_queues_list["Metadata"] = "metadata-queue"

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        video_artifact = S3VideoArtifact(
            footage_id="foo_footage_id",
            tenant_id="test_tenant",
            device_id="test-device",
            raw_s3_path="s3://foo-raw-bucket/test_s3_path.mp4",
            anonymized_s3_path="s3://foo-anonymized-bucket/test_s3_path.mp4",
            rcc_s3_path="s3://rcc-bucket/test_s3_path.mp4",
            timestamp=from_ts,
            end_timestamp=to_ts,
            artifact_id="foo_artifact_id",
            recorder=RecorderType.TRAINING,
            upload_timing=TimeWindow(start=from_ts, end=to_ts),
            recordings=[Recording(recording_id="rec_id", chunk_ids=[1])]
        )

        video_rules: list[str] = []
        decisions: list[DBDecision] = []
        for parameters in rule_parameters:
            video_rules.append(VideoUploadRule(
                tenant=video_artifact.tenant_id,
                raw_file_path=video_artifact.raw_s3_path,
                video_id=video_artifact.artifact_id,
                footage_from=from_ts,
                footage_to=to_ts,
                rule=SelectorRule(
                    rule_name=parameters["rule_name"],
                    rule_version=parameters["rule_version"],
                    origin=parameters["origin"]
                )).stringify())

            decisions.append(DBDecision(rule_name=parameters["rule_name"],
                                        rule_version=parameters["rule_version"],
                                        origin=parameters["origin"],
                                        tenant=video_artifact.tenant_id,
                                        footage_id=video_artifact.footage_id,
                                        footage_from=from_ts,
                                        footage_to=to_ts))

        if len(rule_parameters) == 0:
            video_rules.append(VideoUploadRule(
                tenant=video_artifact.tenant_id,
                raw_file_path=video_artifact.raw_s3_path,
                video_id=video_artifact.artifact_id,
                footage_from=from_ts,
                footage_to=to_ts,
                rule=DEFAULT_RULE).stringify())

            decisions.append(DBDecision(rule_name=DEFAULT_RULE_NAME,
                                        rule_version=DEFAULT_RULE_VERSION,
                                        origin=RuleOrigin.UKNOWN,
                                        tenant=video_artifact.tenant_id,
                                        footage_id=video_artifact.footage_id,
                                        footage_from=from_ts,
                                        footage_to=to_ts))

        DBDecision.objects = Mock(return_value=decisions)

        correlator = Correlator(sqs_controller)

        # WHEN
        success = correlator.correlate_video_rules(video_artifact)

        # THEN
        sqs_controller.send_message.assert_has_calls(
            [call(rule, CONTAINER_NAME) for rule in video_rules])
        assert success

    def test_correlate_snapshot_rules(self):
        # GIVEN
        sqs_controller = Mock()
        sqs_controller.send_message = Mock(return_value=None)

        cont_services = MagicMock()
        cont_services.sqs_queues_list["Metadata"] = "metadata-queue"

        from_ts = datetime.now(timezone.utc) - timedelta(minutes=5)
        to_ts = datetime.now(timezone.utc)
        snap_artifact = SnapshotArtifact(
            footage_id="foo_footage_id",
            device_id="foo_device_id",
            tenant_id="test_tenant",
            raw_s3_path="s3://foo-raw-bucket/test_s3_path.jpeg",
            anonymized_s3_path="s3://foo-anonymized-bucket/test_s3_path.jpeg",
            timestamp=from_ts,
            end_timestamp=to_ts,
            artifact_id="foo_artifact_id",
            recorder=RecorderType.SNAPSHOT,
            upload_timing=TimeWindow(start=from_ts, end=to_ts),
            uuid="some_uuid"
        )

        rule = SnapshotUploadRule(
            tenant=snap_artifact.tenant_id,
            raw_file_path=snap_artifact.raw_s3_path,
            snapshot_id=snap_artifact.artifact_id,
            snapshot_timestamp=snap_artifact.timestamp,
            rule=DEFAULT_RULE
        ).stringify()

        correlator = Correlator(sqs_controller)

        # WHEN
        success = correlator.correlate_snapshot_rules(snap_artifact)

        # THEN
        sqs_controller.send_message.assert_called_once_with(rule, CONTAINER_NAME)
        assert success
