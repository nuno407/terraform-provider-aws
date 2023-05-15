""" tests for artifact injector """
from datetime import datetime

import pytest
import pytz

from base.model.artifacts import (Artifact, IMUArtifact, MetadataArtifact,
                                  RecorderType, SignalsArtifact,
                                  SnapshotArtifact, TimeWindow, VideoArtifact)
from sanitizer.artifact.artifact_injector import MetadataArtifactInjector


@pytest.fixture
def artifact_injector():
    """Return a MetadataArtifactInjector."""
    return MetadataArtifactInjector()


fixture_snapshot = SnapshotArtifact(
    device_id="device_id",
    tenant_id="tenant_id",
    recorder=RecorderType.SNAPSHOT,
    timestamp=datetime.now(tz=pytz.UTC),
    upload_timing=TimeWindow(
        start="2022-12-18T07:37:07.842030994Z",
        end="2022-12-18T07:37:07.842030994Z"),
    uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-e542440d-a6a2-4733-a98b-94a55884b805_1.jpeg"
)

fixture_interior = VideoArtifact(
    device_id="device_id",
    tenant_id="tenant_id",
    recorder=RecorderType.INTERIOR,
    stream_name="stream_name",
    timestamp=datetime.now(tz=pytz.UTC),
    end_timestamp=datetime.now(tz=pytz.UTC),
    upload_timing=TimeWindow(
        start="2022-12-18T07:37:07.842030994Z",
        end="2022-12-18T07:37:07.842030994Z")
)

fixture_training = VideoArtifact(
    device_id="device_id",
    tenant_id="tenant_id",
    recorder=RecorderType.TRAINING,
    stream_name="stream_name",
    timestamp=datetime.now(tz=pytz.UTC),
    end_timestamp=datetime.now(tz=pytz.UTC),
    upload_timing=TimeWindow(
        start="2022-12-18T07:37:07.842030994Z",
        end="2022-12-18T07:37:07.842030994Z")
)


@pytest.mark.parametrize("input_artifact,expected_inject", [
    # don't inject anything for snapshot recorder
    (
        fixture_snapshot,
        [
            SignalsArtifact(
                device_id="device_id",
                tenant_id="tenant_id",
                referred_artifact=fixture_snapshot
            )
        ]
    ),
    # inject signals artifact for interior recorder
    (
        fixture_interior,
        [
            SignalsArtifact(
                device_id="device_id",
                tenant_id="tenant_id",
                referred_artifact=fixture_interior
            )
        ]
    ),
    # inject signals and imu artifacts for training recorder
    (
        fixture_training,
        [
            SignalsArtifact(
                device_id="device_id",
                tenant_id="tenant_id",
                referred_artifact=fixture_training
            ),
            IMUArtifact(
                device_id="device_id",
                tenant_id="tenant_id",
                referred_artifact=fixture_training
            )
        ]
    )
])
@pytest.mark.unit
def test_inject_artifact(input_artifact: Artifact,
                         expected_inject: list[MetadataArtifact],
                         artifact_injector: MetadataArtifactInjector):
    """Test that the artifact injector works as expected."""
    assert artifact_injector.inject(input_artifact) == expected_inject
