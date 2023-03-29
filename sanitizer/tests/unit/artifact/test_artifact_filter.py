""" artifact filter test module. """
from datetime import datetime

import pytest

from base.model.artifacts import (Artifact, RecorderType, SnapshotArtifact,
                                  VideoArtifact)
from sanitizer.artifact.artifact_filter import ArtifactFilter
from sanitizer.config import SanitizerConfig


def _sanitizer_config(tenant_blacklist: list[str], recorder_blacklist: list[str]) -> SanitizerConfig:
    return SanitizerConfig(
        input_queue="foo",
        topic_arn="bar",
        db_name="db-foo",
        message_collection="foobar-collection",
        recorder_blacklist=recorder_blacklist,
        tenant_blacklist=tenant_blacklist,
        training_whitelist=[]
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
            timestamp=datetime.now(),
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
            timestamp=datetime.now(),
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
            tenant_id=None,
            device_id="DEV03",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(),
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
            timestamp=datetime.now(),
            end_timestamp=datetime.now()
        ),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.FRONT]
        ),
        False
    ),
    # no recorder specified
    (
        VideoArtifact(
            stream_name="foobar5",
            tenant_id="deepsensation",
            device_id="DEV05",
            recorder=None,
            timestamp=datetime.now(),
            end_timestamp=datetime.now()
        ),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.FRONT]
        ),
        True
    ),
    # blacklisted tenant and recorder
    (
        VideoArtifact(
            stream_name="foobar5",
            tenant_id="deepsensation",
            device_id="DEV05",
            recorder=RecorderType.TRAINING,
            timestamp=datetime.now(),
            end_timestamp=datetime.now()
        ),
        _sanitizer_config(
            tenant_blacklist=["deepsensation"],
            recorder_blacklist=[RecorderType.TRAINING]
        ),
        False
    )
])
def test_artifact_filter_is_relevant(artifact: Artifact,
                                     config: SanitizerConfig,
                                     expected: bool):
    assert ArtifactFilter().is_relevant(artifact, config) == expected
