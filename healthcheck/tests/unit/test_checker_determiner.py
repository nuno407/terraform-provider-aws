"""Tests for CheckerDeterminer"""

from datetime import datetime
from unittest.mock import Mock

from pytest import mark
from pytz import UTC

from base.model.artifacts import (Artifact, OperatorArtifact, RecorderType,
                                  S3VideoArtifact, SnapshotArtifact)
from healthcheck.checker.checker_determiner import CheckerDeterminer
from healthcheck.checker.common import ArtifactChecker

interior_checker = Mock()
training_checker = Mock()
snapshot_checker = Mock()
operator_checker = Mock()

common_properties = {
    "artifact_id": "bar",
    "raw_s3_path": "s3://raw/foo/bar.something",
    "anonymized_s3_path": "s3://anonymized/foo/bar.something",
    "tenant_id": "tenant_id",
    "device_id": "device_id",
    "timestamp": datetime.now(tz=UTC),
    "end_timestamp": datetime.now(tz=UTC),
    "upload_timing": {"start": datetime.now(tz=UTC), "end": datetime.now(tz=UTC)}
}

interior_video = S3VideoArtifact(**common_properties,
                                 recorder=RecorderType.INTERIOR,
                                 rcc_s3_path="s3://foo/bar",
                                 footage_id="foo",
                                 recordings=[])
training_video = S3VideoArtifact(**common_properties,
                                 recorder=RecorderType.TRAINING,
                                 rcc_s3_path="s3://foo/bar",
                                 footage_id="foo",
                                 recordings=[])
snapshot = SnapshotArtifact(**common_properties, recorder=RecorderType.SNAPSHOT, uuid="bar")
operator_feedback = OperatorArtifact(tenant_id="tenant",
                                     device_id="device",
                                     event_timestamp=datetime.now(tz=UTC),
                                     operator_monitoring_start=datetime.now(tz=UTC),
                                     operator_monitoring_end=datetime.now(tz=UTC))


class TestCheckerDeterminer:
    @mark.unit()
    @mark.parametrize(["input_artifact", "expected_checker"], [
        (interior_video, interior_checker),
        (training_video, training_checker),
        (snapshot, snapshot_checker),
        (operator_feedback, operator_checker)
    ])
    def test_correct_checker_is_determined(self, input_artifact: Artifact, expected_checker: ArtifactChecker):
        # GIVEN
        checker_determiner = CheckerDeterminer(
            training_checker=training_checker,
            interior_checker=interior_checker,
            snapshot_checker=snapshot_checker,
            operator_checker=operator_checker)

        # WHEN
        actual_checker = checker_determiner.get_checker(input_artifact)

        # THEN
        assert actual_checker == expected_checker
