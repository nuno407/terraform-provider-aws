""" tests for artifact injector """
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import (Artifact, IMUArtifact, MetadataArtifact,
                                  MultiSnapshotArtifact,
                                  PreviewSignalsArtifact, RecorderType,
                                  S3VideoArtifact, SignalsArtifact,
                                  SnapshotArtifact, TimeWindow)
from sanitizer.artifact.artifact_injector import MetadataArtifactInjector


@pytest.fixture
def artifact_injector():
    """Return a MetadataArtifactInjector."""
    return MetadataArtifactInjector()


device_and_tenant_and_timings = {
    "device_id": "device_id",
    "tenant_id": "tenant_id",
    "timestamp": datetime.now(tz=UTC) - timedelta(minutes=2),
    "end_timestamp": datetime.now(tz=UTC) - timedelta(minutes=1),
    "upload_timing": TimeWindow(
        start=datetime.now(tz=UTC) - timedelta(seconds=10),
        end=datetime.now(tz=UTC) - timedelta(seconds=5)
    )
}

fixture_snapshot = SnapshotArtifact(
    **device_and_tenant_and_timings,
    recorder=RecorderType.SNAPSHOT,
    uuid="TrainingMultiSnapshot_TrainingMultiSnapshot-e542440d-a6a2-4733-a98b-94a55884b805_1.jpeg"
)

fixture_interior = S3VideoArtifact(
    footage_id="d568f40f-7a3c-5d64-a7a8-d381f70ca973",
    rcc_s3_path="s3://rcc-bucket/key",
    device_id="device_id",
    tenant_id="tenant_id",
    recorder=RecorderType.INTERIOR,
    timestamp=datetime.now(tz=UTC),
    end_timestamp=datetime.now(tz=UTC),
    upload_timing=TimeWindow(
        start="2022-12-18T07:37:07.842030994Z",
        end="2022-12-18T07:37:07.842030994Z")
)

fixture_training = S3VideoArtifact(
    footage_id="d517f72f-4c5f-58c0-bfcc-8c97ae8e9107",
    rcc_s3_path="s3://rcc-bucket/key",
    device_id="device_id",
    tenant_id="tenant_id",
    recorder=RecorderType.TRAINING,
    timestamp=datetime.now(tz=UTC),
    end_timestamp=datetime.now(tz=UTC),
    upload_timing=TimeWindow(
        start="2022-12-18T07:37:07.842030994Z",
        end="2022-12-18T07:37:07.842030994Z")
)
fixture_preview = SnapshotArtifact(
    **device_and_tenant_and_timings,
    recorder=RecorderType.INTERIOR_PREVIEW,
    uuid="some-uuid.jpeg"
)

fixture_multi_preview = MultiSnapshotArtifact(
    **device_and_tenant_and_timings,
    recorder=RecorderType.INTERIOR_PREVIEW,
    chunks=[fixture_preview],
    recording_id="InteriorRecorderPreview-145c7e01-5278-4f2b-8637-40f3f027a4b8",
)


@pytest.mark.parametrize("input_artifact,expected_inject", [
    # don't inject anything for snapshot recorder
    (
        fixture_snapshot,
        [
            SignalsArtifact(
                **device_and_tenant_and_timings,
                referred_artifact=fixture_snapshot
            )
        ]
    ),
    # inject signals artifact for interior recorder
    (
        fixture_interior,
        [
            SignalsArtifact(
                **device_and_tenant_and_timings,
                referred_artifact=fixture_interior
            )
        ]
    ),
    # inject signals and imu artifacts for training recorder
    (
        fixture_training,
        [
            SignalsArtifact(
                **device_and_tenant_and_timings,
                referred_artifact=fixture_training
            ),
            IMUArtifact(
                **device_and_tenant_and_timings,
                referred_artifact=fixture_training
            )
        ]
    ),
    # inject preview signals artifact for interior preview recorder
    (
        fixture_multi_preview,
        [
            PreviewSignalsArtifact(
                **device_and_tenant_and_timings,
                referred_artifact=fixture_multi_preview
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
