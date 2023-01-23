import sys
from unittest.mock import Mock, patch

import pytest

from healthcheck.voxel_client import VoxelClient, VoxelDataset


@pytest.mark.unit
class TestVoxelClient():
    """test voxel client."""

    def test_get_num_entries_error_invalid_dataset(self, fiftyone: Mock):
        fo = fiftyone
        fo.dataset_exists = Mock(return_value=False)
        voxel_client = VoxelClient()
        with pytest.raises(ValueError):
            voxel_client.get_num_entries("test.jpeg", VoxelDataset.SNAPSHOTS)

    def test_get_num_entries_success(self, fiftyone: Mock):
        fo = fiftyone
        fo.dataset_exists = Mock(return_value=True)
        dataset_mock = Mock()
        fo.load_dataset = Mock(return_value=dataset_mock)
        view_mock = Mock()
        view_mock.count = Mock(return_value=1)
        dataset_mock.select_by = Mock(return_value=view_mock)
        voxel_client = VoxelClient()
        num_entries = voxel_client.get_num_entries(
            "test.jpeg", VoxelDataset.SNAPSHOTS)

        fo.load_dataset.assert_called_once_with(VoxelDataset.SNAPSHOTS.value)
        dataset_mock.select_by.assert_called_once_with(
            "filepath",
            "test.jpeg"
        )
        assert num_entries == 1
