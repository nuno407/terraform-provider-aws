from datetime import datetime
from unittest.mock import Mock

import pytest

from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import VoxelEntryNotPresent, VoxelEntryNotUnique
from healthcheck.model import S3Params, SnapshotArtifact
from healthcheck.voxel_client import VoxelDataset


@pytest.mark.unit
class TestVoxelFiftyOneController():
    """Unit tests for voxel fiftyone controller."""

    @pytest.fixture
    def s3_params(self) -> S3Params:
        return S3Params(
            s3_bucket_anon="test-anon",
            s3_bucket_raw="test-raw",
            s3_dir="test-dir"
        )

    @pytest.fixture
    def snapshot_artifact(self) -> SnapshotArtifact:
        return SnapshotArtifact(
            tenant_id="test",
            device_id="test",
            uuid="test",
            timestamp=datetime.now()
        )

    def _assemble_path(self, bucket: str, dir: str, key: str, ext: str) -> str:
        return f"s3://{bucket}/{dir}/{key}_anonymized.{ext}"

    def test_is_fiftyone_entry_present_or_raise_success(self, snapshot_artifact: SnapshotArtifact, s3_params: S3Params):
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__gt__ = Mock(return_value=False)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact, VoxelDataset.SNAPSHOTS)
        voxel_client.get_num_entries.assert_called_once_with(
            self._assemble_path(s3_params.s3_bucket_anon, s3_params.s3_dir, snapshot_artifact.artifact_id, "jpeg"),
            VoxelDataset.SNAPSHOTS
        )

    def test_is_fiftyone_entry_present_or_raise_error_empty(
            self, snapshot_artifact: SnapshotArtifact, s3_params: S3Params):
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__eq__ = Mock(return_value=True)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        with pytest.raises(VoxelEntryNotPresent):
            voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact, VoxelDataset.SNAPSHOTS)

    def test_is_fiftyone_entry_present_or_raise_error_multiple(
            self, snapshot_artifact: SnapshotArtifact, s3_params: S3Params):
        voxel_client = Mock()
        result_mock = Mock()
        result_mock.__gt__ = Mock(return_value=True)
        voxel_client.get_num_entries = Mock(return_value=result_mock)
        voxel_controller = VoxelFiftyOneController(s3_params, voxel_client)
        with pytest.raises(VoxelEntryNotUnique):
            voxel_controller.is_fiftyone_entry_present_or_raise(snapshot_artifact, VoxelDataset.SNAPSHOTS)
