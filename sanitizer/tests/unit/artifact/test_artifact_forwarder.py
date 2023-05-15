""" Test artifact forwarder module. """
from datetime import datetime
from unittest.mock import Mock

import pytest
from pytz import UTC

from base.model.artifacts import (RecorderType, SnapshotArtifact, TimeWindow,
                                  VideoArtifact)
from sanitizer.artifact.artifact_forwarder import ArtifactForwarder


@pytest.mark.unit
@pytest.mark.parametrize("artifact", [
    (
        VideoArtifact(
            stream_name="mystream01",
            tenant_id="123456",
            device_id="12345",
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            recorder=RecorderType.INTERIOR,
            upload_timing=TimeWindow(
                start="2022-12-18T07:37:07.842030994Z",
                end="2022-12-18T07:37:07.842030994Z")
        )
    ),
    (
        SnapshotArtifact(
            uuid="f8502029-9de0-4c19-8a85-1b223bdd08da",
            tenant_id="deepsensation",
            device_id="DEV_01",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start="2022-12-18T07:37:07.842030994Z",
                end="2022-12-18T07:37:07.842030994Z")
        )
    )
])
def test_artifact_forwarder(artifact):
    sns_controller_mock = Mock()
    sns_controller_mock.publish = Mock(return_value=None)
    artifact_forwarder = ArtifactForwarder(aws_sns_controller=sns_controller_mock)
    artifact_forwarder.publish(artifact=artifact)
    sns_controller_mock.publish.assert_called_once_with(artifact.stringify())
