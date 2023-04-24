import pytest
from unittest.mock import Mock, MagicMock
from metadata.consumer.voxel.metadata_artifacts import Frame
from metadata.consumer.voxel.voxel_metadata_loader import VoxelSnapshotMetadataLoader
import sys
import json
import os

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

METADATA_LOCATION = os.path.join(CURRENT_LOCATION, "test_data", "metadata_data")


def load_json(file_name: str) -> dict:
    """
    Loads a local json file

    Args:
        file_name (str): _description_

    Returns:
        dict: _description_
    """
    file_path = os.path.join(METADATA_LOCATION, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_frame_pydantic(file_name: str) -> Frame:
    """
    Load Frame pydantic model

    Args:
        file_name (str): _description_

    Returns:
        Frame: _description_
    """
    metadata_format = load_json(file_name)
    return Frame(**metadata_format)


@pytest.mark.unit
class TestVoxelSnapshotMetadataLoader:

    @pytest.mark.unit
    def test_load(self, voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader):
        """Test the load function"""
        # GIVEN
        frame = Mock()
        voxel_snapshot_metadata_loader.set_sample(Mock())
        voxel_snapshot_metadata_loader.load_bbox = Mock()
        voxel_snapshot_metadata_loader.load_classifications = Mock()
        voxel_snapshot_metadata_loader.load_pose_keypoints = Mock()

        # WHEN
        voxel_snapshot_metadata_loader.load(frame)

        # THEN
        voxel_snapshot_metadata_loader.load_bbox.assert_called_once_with(frame)
        voxel_snapshot_metadata_loader.load_classifications.assert_called_once_with(frame)
        voxel_snapshot_metadata_loader.load_pose_keypoints.assert_called_once_with(frame)

    @pytest.mark.unit
    @pytest.mark.parametrize("frame,expected_keypoints", [
        (
            load_frame_pydantic("snapshot_pose_pydantic.json"),
            load_json("snapshot_pose_voxel.json")
        ),
        (
            Frame(persons=[], bboxes=[], classifications=[], width=1280, height=720),
            {}
        )

    ])
    def test_load_pose_keypoints(
            self,
            frame: Frame,
            expected_keypoints: dict,
            voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader,
            fo_sample: MagicMock):
        """Test insertion of keypoints"""
        # GIVEN
        voxel_snapshot_metadata_loader.set_sample(fo_sample)

        # WHEN
        voxel_snapshot_metadata_loader.load_pose_keypoints(frame)

        # THEN

        json_fo_sample = json.dumps(fo_sample)  # Needed to convert tuples to lists
        assert json.loads(json_fo_sample) == expected_keypoints

    @pytest.mark.unit
    @pytest.mark.parametrize("frame,expected_classifications", [
        (
            load_frame_pydantic("snapshot_classification_pydantic.json"),
            load_json("snapshot_classification_voxel.json")
        ),
        (
            Frame(persons=[], bboxes=[], classifications=[], width=1280, height=720),
            {}
        )

    ])
    def test_load_classifications(
            self,
            frame: Frame,
            expected_classifications: dict,
            voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader,
            fo_sample: MagicMock):
        """Test insertion of keypoints"""

        # GIVEN
        voxel_snapshot_metadata_loader.set_sample(fo_sample)

        # WHEN
        voxel_snapshot_metadata_loader.load_classifications(frame)

        # THEN
        json_fo_sample = json.dumps(fo_sample)  # Needed to convert tuples to lists

        assert json.loads(json_fo_sample) == expected_classifications

    @pytest.mark.unit
    @pytest.mark.parametrize("frame,expected_bbox", [
        (
            load_frame_pydantic("snapshot_bbox_pydantic.json"),
            load_json("snapshot_bbox_voxel.json")
        )
    ])
    def test_load_bbox(
            self,
            frame: Frame,
            expected_bbox: dict,
            voxel_snapshot_metadata_loader: VoxelSnapshotMetadataLoader,
            fo_sample: MagicMock):
        """Test insetion of keypoints"""

        # GIVEN
        voxel_snapshot_metadata_loader.set_sample(fo_sample)

        # WHEN
        voxel_snapshot_metadata_loader.load_bbox(frame)

        # THEN
        json_fo_sample = json.dumps(fo_sample)  # Needed to convert tuples to lists
        assert json.loads(json_fo_sample) == expected_bbox
