import pytest
import fiftyone as fo
from unittest.mock import Mock, patch
from artifact_api.voxel.voxel_snapshot import VoxelSnapshot
from artifact_api.voxel.voxel_metadata_transformer import VoxelMetadataFrameFields


class TestVoxelSnapshot:
    """Tests for VoxelSnapshot"""

    @pytest.fixture
    def voxel_snapshot(self) -> VoxelSnapshot:
        """Fixture for VoxelSnapshot"""
        return VoxelSnapshot()

    @pytest.mark.unit
    def test_load_metadata(self, voxel_snapshot: VoxelSnapshot):
        """Test VoxelSnapshot metadata loading"""

        # GIVEN
        dataset = Mock()

        anon_path = "s3://bucket/tenant/file.png"
        tenant_id = "tenant"
        fields = VoxelMetadataFrameFields(Mock(), Mock())

        mock_upsert_sample = Mock()
        VoxelSnapshot._upsert_sample = mock_upsert_sample

        values_to_set = {
            voxel_snapshot.Fields.KEYPOINTS.value: fields.keypoints,
            voxel_snapshot.Fields.CLASSIFICATIONS.value: fields.classifications
        }

        # THEN
        voxel_snapshot.load_metadata(dataset, anon_path, tenant_id, fields)

        VoxelSnapshot._upsert_sample.assert_called_once_with(
            tenant_id=tenant_id,
            dataset=dataset,
            anonymized_filepath=anon_path,
            values_to_set=values_to_set
        )
