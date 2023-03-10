import sys
from unittest.mock import Mock, patch

import pytest

from healthcheck.voxel_client import VoxelClient


@pytest.mark.unit
class TestVoxelClient():
    """test voxel client."""

    def test_get_num_entries_error_invalid_dataset(self, fiftyone: Mock):
        # GIVEN
        fo = fiftyone
        fo.dataset_exists = Mock(return_value=False)
        voxel_client = VoxelClient()
        # THEN
        with pytest.raises(ValueError):
            # WHEN
            voxel_client.get_num_entries("test.jpeg", "collection")

    def test_get_num_entries_success(self, fiftyone: Mock):
        # GIVEN
        fo = fiftyone
        fo.dataset_exists = Mock(return_value=True)
        dataset_mock = Mock()
        fo.load_dataset = Mock(return_value=dataset_mock)
        view_mock = Mock()
        view_mock.count = Mock(return_value=1)
        dataset_mock.select_by = Mock(return_value=view_mock)
        voxel_client = VoxelClient()
        # WHEN
        num_entries = voxel_client.get_num_entries(
            "test.jpeg", "collection")
        # THEN
        fo.load_dataset.assert_called_once_with("collection")
        dataset_mock.select_by.assert_called_once_with(
            "filepath",
            "test.jpeg"
        )
        assert num_entries == 1
