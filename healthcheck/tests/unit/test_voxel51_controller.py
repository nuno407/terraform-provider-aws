from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
from kink import di
from pytz import UTC

from base.model.artifacts import (RecorderType, S3VideoArtifact,
                                  SnapshotArtifact, TimeWindow)
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import VoxelEntryNotPresent, VoxelEntryNotUnique
from healthcheck.model import S3Params
from healthcheck.tenant_config import DatasetMappingConfig, TenantConfig


@pytest.mark.unit
class TestVoxelFiftyOneController():
    """Unit tests for voxel fiftyone controller."""

    @pytest.fixture
    def s3_params(self) -> S3Params:
        return S3Params(
            s3_bucket_anon="test-anon",
            s3_bucket_raw="test-raw"
        )

    @pytest.fixture
    def snapshot_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="test",
            device_id="test",
            uuid="test",
            recorder=RecorderType.SNAPSHOT,
            timestamp=datetime.now(tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC),
                end=datetime.now(tz=UTC))
        )

    @pytest.fixture
    def video_artifact(self) -> S3VideoArtifact:
        return S3VideoArtifact(
            tenant_id="test",
            device_id="device-test",
            footage_id="footage-test",
            rcc_s3_path="s3://test/test",
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.now(tz=UTC) - timedelta(hours=1),
            end_timestamp=datetime.now(tz=UTC) - timedelta(minutes=30),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC) - timedelta(hours=1),
                end=datetime.now(tz=UTC) - timedelta(minutes=30))
        )

    @pytest.fixture(autouse=True)
    def initialize_config(self):
        tenant_config = TenantConfig.load_config_from_yaml_file("./config.yaml")
        di[DatasetMappingConfig] = tenant_config.dataset_mapping

    def _assemble_path(self, bucket: str, dir: str, key: str, ext: str) -> str:
        return f"s3://{bucket}/{dir}/{key}_anonymized.{ext}"

    @pytest.mark.parametrize("tenant,expected_dataset",
                             [("test", "Debug_Lync_snapshots"), ("datanauts", "RC-datanauts_snapshots")])
    def test_is_fiftyone_snapshot_present(self, snapshot_artifact: SnapshotArtifact, tenant: str,
                                          expected_dataset: str, s3_params: S3Params):
        # GIVEN
        snapshot_artifact.tenant_id = tenant
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__gt__ = Mock(return_value=False)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        # WHEN
        voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact)
        # THEN
        voxel_client.get_num_entries.assert_called_once_with(
            self._assemble_path(
                s3_params.s3_bucket_anon,
                snapshot_artifact.tenant_id,
                snapshot_artifact.artifact_id,
                "jpeg"),
            expected_dataset)

    @pytest.mark.parametrize("tenant,expected_dataset", [("test", "Debug_Lync"), ("datanauts", "RC-datanauts")])
    def test_is_fiftyone_video_present(self, video_artifact: S3VideoArtifact, tenant: str,
                                       expected_dataset: str, s3_params: S3Params):
        # GIVEN
        video_artifact.tenant_id = tenant
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__gt__ = Mock(return_value=False)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        # WHEN
        voxel_controller.is_fiftyone_entry_present_or_raise(video_artifact)
        # THEN
        voxel_client.get_num_entries.assert_called_once_with(
            self._assemble_path(
                s3_params.s3_bucket_anon,
                video_artifact.tenant_id,
                video_artifact.artifact_id,
                "mp4"),
            expected_dataset)

    def test_is_fiftyone_entry_present_or_raise_error_empty(
            self, snapshot_artifact: SnapshotArtifact, s3_params: S3Params):
        # GIVEN
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__eq__ = Mock(return_value=True)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        # THEN
        with pytest.raises(VoxelEntryNotPresent):
            # WHEN
            voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact)

    def test_is_fiftyone_entry_present_or_raise_error_multiple(
            self, snapshot_artifact: SnapshotArtifact, s3_params: S3Params):
        # GIVEN
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__gt__ = Mock(return_value=True)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        # THEN
        with pytest.raises(VoxelEntryNotUnique):
            # WHEN
            voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact)
