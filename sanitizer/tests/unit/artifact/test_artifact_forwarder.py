""" Test artifact forwarder module. """
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from pytz import UTC

from base.model.artifacts import (RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from sanitizer.artifact.artifact_forwarder import ArtifactForwarder

timings = {
    "timestamp": datetime.now(tz=UTC) - timedelta(minutes=2),
    "end_timestamp": datetime.now(tz=UTC) - timedelta(minutes=1),
    "upload_timing": TimeWindow(
        start=datetime.now(tz=UTC) - timedelta(seconds=10),
        end=datetime.now(tz=UTC) - timedelta(seconds=5)
    )
}

@pytest.mark.unit
@pytest.mark.parametrize("artifact", [
    (
        S3VideoArtifact(
            footage_id="bd94818c-b992-50fa-8556-ed7732aed924",
            rcc_s3_path="s3://rcc-bucket/key",
            tenant_id="123456",
            device_id="12345",
            recorder=RecorderType.INTERIOR,
            **timings
        )
    ),
    (
        SnapshotArtifact(
            uuid="f8502029-9de0-4c19-8a85-1b223bdd08da",
            tenant_id="deepsensation",
            device_id="DEV_01",
            recorder=RecorderType.SNAPSHOT,
            **timings
        )
    )
])
def test_artifact_forwarder(artifact):
    sns_controller_mock = Mock()
    sns_controller_mock.publish = Mock(return_value=None)
    artifact_forwarder = ArtifactForwarder(aws_sns_controller=sns_controller_mock)
    artifact_forwarder.publish(artifact=artifact)
    sns_controller_mock.publish.assert_called_once_with(artifact.stringify())
