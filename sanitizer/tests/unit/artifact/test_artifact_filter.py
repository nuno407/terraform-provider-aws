""" artifact filter test module. """
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  TimeWindow, VideoArtifact)
from sanitizer.artifact.artifact_filter import ArtifactFilter
from sanitizer.config import SanitizerConfig


def _sanitizer_config(tenant_blacklist: list[str], recorder_blacklist: list[str]) -> SanitizerConfig:
    return SanitizerConfig(
        input_queue="foo",
        topic_arn="bar",
        db_name="db-foo",
        message_collection="foobar-collection",
        recorder_blacklist=recorder_blacklist,
        tenant_blacklist=tenant_blacklist
    )


@pytest.mark.unit
@pytest.mark.parametrize("artifact,config,expected", [
    # good artifact
    (
        SnapshotArtifact(
            uuid="foobar1",
            tenant_id="datanauts",
            device_id="DEV01",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[]
        ),
        True
    ),
    # blacklisted tenant
    (
        SnapshotArtifact(
            uuid="foobar2",
            tenant_id="datanauts",
            device_id="DEV02",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[]
        ),
        False
    ),
    # no tenant specified
    (
        SnapshotArtifact(
            uuid="foobar3",
            tenant_id="",
            device_id="DEV03",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[]
        ),
        True
    ),
    # blacklisted recorder
    (
        VideoArtifact(
            stream_name="foobar4",
            tenant_id="deepsensation",
            device_id="DEV04",
            recorder=RecorderType.FRONT,
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.FRONT.value]
        ),
        False
    ),
    # multiple blacklisted recorder
    (
        VideoArtifact(
            stream_name="foobar4",
            tenant_id="deepsensation",
            device_id="DEV04",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.INTERIOR.value, RecorderType.FRONT.value]
        ),
        False
    ),
    # blacklisted tenant and recorder
    (
        VideoArtifact(
            stream_name="foobar5",
            tenant_id="deepsensation",
            device_id="DEV05",
            recorder=RecorderType.TRAINING,
            timestamp=datetime.now(tz=UTC),
            end_timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        ),
        _sanitizer_config(
            tenant_blacklist=["deepsensation"],
            recorder_blacklist=[RecorderType.TRAINING.value]
        ),
        False
    )
])
def test_artifact_filter_is_relevant(artifact: Artifact,
                                     config: SanitizerConfig,
                                     expected: bool):
    assert ArtifactFilter().is_relevant(artifact, config) == expected
