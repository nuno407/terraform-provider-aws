""" artifact filter test module. """
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import ANY, Mock
import pytest
from pytz import UTC

from base.model.artifacts import (Artifact, RecorderType, Recording,
                                  S3VideoArtifact, SnapshotArtifact,
                                  TimeWindow, IMUArtifact)
from sanitizer.artifact.artifact_filter import ArtifactFilter
from sanitizer.config import SanitizerConfig
from sanitizer.device_info_db_client import DeviceInfoDBClient
from sanitizer.models import DeviceInformation


def _sanitizer_config(tenant_blacklist: list[str],
                      recorder_blacklist: list[str],
                      version_blacklist: dict[type[Artifact],
                                              set[str]] = {}) -> SanitizerConfig:
    return SanitizerConfig(
        input_queue="foo",
        metadata_queue="md_q",
        topic_arn="bar",
        db_name="db-foo",
        message_collection="foobar-collection",
        recorder_blacklist=recorder_blacklist,
        tenant_blacklist=tenant_blacklist,
        devcloud_anonymized_bucket="devcloud-anonymized-bucket",
        devcloud_raw_bucket="devcloud-raw-bucket",
        device_info_collection="device-collection",
        version_blacklist=version_blacklist
    )


timings = {
    "timestamp": datetime.now(tz=UTC) - timedelta(minutes=2),
    "end_timestamp": datetime.now(tz=UTC) - timedelta(minutes=1),
    "upload_timing": TimeWindow(
        start=datetime.now(tz=UTC) - timedelta(seconds=10),
        end=datetime.now(tz=UTC) - timedelta(seconds=5)
    )
}


def video_artifact() -> S3VideoArtifact:
    return S3VideoArtifact(
        artifact_id="bar",
        raw_s3_path="s3://raw/foo/bar.something",
        anonymized_s3_path="s3://anonymized/foo/bar.something",
        footage_id="8d98a113-2a74-50e8-a706-6ae854d59923",
        rcc_s3_path="s3://rcc-bucket/key",
        tenant_id="deepsensation",
        device_id="DEV04",
        recorder=RecorderType.FRONT,
        recordings=[Recording(recording_id="TrainingRecorder-abc", chunk_ids=[1, 2, 3])],
        **timings
    )


def snapshot_artifact() -> SnapshotArtifact:
    return SnapshotArtifact(
        artifact_id="bar",
        raw_s3_path="s3://raw/foo/bar.something",
        anonymized_s3_path="s3://anonymized/foo/bar.something",
        uuid="foobar1",
        tenant_id="datanauts",
        device_id="DEV01",
        recorder=RecorderType.SNAPSHOT,
        **timings
    )


@pytest.mark.unit
@pytest.mark.parametrize("artifact,test_config,expected", [
    # good artifact
    (
        snapshot_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[]
        ),
        True
    ),
    # blacklisted tenant
    (
        snapshot_artifact(),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[]
        ),
        False
    ),
    # no tenant specified
    (
        snapshot_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[]
        ),
        True
    ),
    # blacklisted recorder
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.FRONT.value]
        ),
        False
    ),
    # multiple blacklisted recorder
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=["datanauts"],
            recorder_blacklist=[RecorderType.INTERIOR.value, RecorderType.FRONT.value]
        ),
        False
    ),
    # blacklisted tenant and recorder
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=["deepsensation"],
            recorder_blacklist=[RecorderType.TRAINING.value]
        ),
        False
    )
])
def test_artifact_filter_is_relevant(artifact: Artifact,
                                     test_config: SanitizerConfig,
                                     expected: bool,
                                     mock_device_db_client: DeviceInfoDBClient):
    mock_device_db_client.get_latest_device_information.assert_not_called()
    assert ArtifactFilter(mock_device_db_client, test_config).is_relevant(artifact) == expected


@pytest.mark.unit
@pytest.mark.parametrize("artifact,test_config,device_version,assert_db_query,expected", [
    # Test blacklisted artifact
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"S3VideoArtifact": ["1.8.0"]}
        ),
        "1.8.0",
        True,
        False
    ),
    # Test when another artifact is blacklisted
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"PreviewSignalsArtifact": ["1.8.0"]}
        ),
        "1.8.0",
        False,
        True
    ),
    # Test when a diferent version is blacklisted
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"S3VideoArtifact": ["1.8.0"]}
        ),
        "1.7.0",
        True,
        True
    ),
    # Test when the device is not in the databse yet
    (
        video_artifact(),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"S3VideoArtifact": ["1.8.0"]}
        ),
        None,
        True,
        True
    ),
    # Test for an artifact without timestamp not in the blacklist
    (
        IMUArtifact(
            tenant_id="deepsensation",
            device_id="DEV04",
            referred_artifact=Mock(spec=S3VideoArtifact)
        ),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"PreviewSignalsArtifact": ["1.8.0"]}
        ),
        "1.8.0",
        False,
        True
    ),
    # Test for an artifact without timestamp in the blacklist
    (
        IMUArtifact(
            tenant_id="deepsensation",
            device_id="DEV04",
            referred_artifact=Mock(spec=S3VideoArtifact)
        ),
        _sanitizer_config(
            tenant_blacklist=[],
            recorder_blacklist=[],
            version_blacklist={"IMUArtifact": ["1.8.0"]}
        ),
        "1.8.0",
        True,
        False
    )
])
def test_artifact_filter_is_relevant_device_versions(
        artifact: Artifact,
        test_config: SanitizerConfig,
        device_version: Optional[str],
        assert_db_query: bool,
        expected: bool,
        mock_device_db_client: DeviceInfoDBClient):

    mocked_date = datetime(year=2023, month=10, day=1)
    filter = ArtifactFilter(mock_device_db_client, test_config)

    if device_version is not None:
        mocked_device_info = DeviceInformation(
            ivscar_version=device_version,
            timestamp=mocked_date,
            tenant_id=artifact.tenant_id,
            device_id=artifact.device_id)
    else:
        mocked_device_info = None
    mock_device_db_client.get_latest_device_information.return_value = mocked_device_info

    result = filter.is_relevant(artifact)

    if assert_db_query:
        if hasattr(artifact, "timestamp"):
            mock_device_db_client.get_latest_device_information.assert_called_once_with(
                artifact.device_id, artifact.tenant_id, timings["timestamp"])
        else:
            mock_device_db_client.get_latest_device_information.assert_called_once_with(
                artifact.device_id, artifact.tenant_id, ANY)
    else:
        mock_device_db_client.get_latest_device_information.assert_not_called()

    assert result == expected
