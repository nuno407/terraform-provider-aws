import pytest
import pytz
from datetime import datetime
from unittest.mock import Mock, MagicMock
from typing import Any

import fiftyone as fo

from base.model.metadata.media_metadata import MediaFrame, MediaMetadata, Resolution, ObjectList, PersonDetails, MediaKeyPoint, MediaKeypointName, Classification
from base.voxel.constants import VOXEL_KEYPOINTS_LABELS, VoxelKeyPointLabel
from artifact_api.exceptions import VoxelSnapshotMetadataError
from artifact_api.voxel.utils import VoxelKPMapper
from artifact_api.voxel.voxel_metadata_transformer import VoxelMetadataTransformer, VoxelMetadataFrameFields


def get_index_of_keypoint(keypoint_name: VoxelKeyPointLabel) -> int:
    """Get index of voxel point"""
    return VOXEL_KEYPOINTS_LABELS.index(keypoint_name)


def assert_contain_subset_dict(lst: list[dict[Any, Any]], subset: dict[Any, Any]):
    """ Assert that a list of dicts contains an element that contains the same key-value pairs as the subset """
    for each_dict in lst:
        if all(k in each_dict and each_dict[k] == v for k, v in subset.items()):
            return True

    assert False, f"List does not contain subset: {subset}"


class TestVoxelMetadataTransformer():

    @pytest.fixture
    def kp_mapper(self) -> VoxelKPMapper:
        """Fixture for VoxelKPMapper"""
        return VoxelKPMapper()

    @pytest.fixture
    def metadata_transformer(self, kp_mapper: VoxelKPMapper) -> VoxelMetadataTransformer:
        """Fixture for VoxelMetadataTransformer"""
        return VoxelMetadataTransformer(kp_mapper)

    @pytest.mark.unit
    def test_get_pose_keypoints(self, metadata_transformer: VoxelMetadataTransformer):
        """Test VoxelMetadataTransformer get_pose_keypoints"""

        # GIVEN
        person_1 = PersonDetails(Confidence=0.2, keypoints=[
            MediaKeyPoint[MediaKeypointName](name=MediaKeypointName.RIGHT_ELBOW, x=200, y=300, conf=0.3),
            MediaKeyPoint[MediaKeypointName](name=MediaKeypointName.LEFT_EAR, x=354, y=400, conf=0.6),
            MediaKeyPoint[MediaKeypointName](name=MediaKeypointName.LEFT_SHOULDER, x=0, y=0, conf=0),
        ])

        person_2 = PersonDetails(Confidence=0.3, keypoints=[
            MediaKeyPoint[MediaKeypointName](name=MediaKeypointName.RIGHT_ELBOW, x=200, y=300, conf=0),
            MediaKeyPoint[MediaKeypointName](name=MediaKeypointName.LEFT_EAR, x=354, y=400, conf=0.6),
        ])

        mf = MediaFrame(
            number=1,
            timestamp=datetime(
                year=2023,
                month=10,
                day=20,
                hour=20,
                tzinfo=pytz.UTC),
            object_list=ObjectList(
                personDetail=[
                    person_1,
                    person_2]))

        # WHEN
        keypoints = metadata_transformer.get_pose_keypoints(mf, Resolution(width=1920, height=1080))

        person_1_keypoints = keypoints.keypoints[0]
        person_2_keypoints = keypoints.keypoints[1]

        # THEN
        assert len(keypoints.keypoints) == 2

        assert person_1_keypoints.label == "Person 0"
        assert len(person_1_keypoints.points) == len(VOXEL_KEYPOINTS_LABELS)
        assert person_1_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.LEFT_ANKLE)] == [None, None]
        assert person_1_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.RIGHT_ELBOW)] == pytest.approx([
            0.10416666666666667, 0.2777777777777778])
        assert person_1_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.LEFT_EAR)] == pytest.approx([
            0.184375, 0.37037037037037035])
        assert person_1_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.LEFT_SHOULDER)] == [None, None]

        assert person_2_keypoints.label == "Person 1"
        assert len(person_2_keypoints.points) == len(VOXEL_KEYPOINTS_LABELS)
        assert person_2_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.RIGHT_ANKLE)] == [None, None]
        assert person_2_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.RIGHT_ELBOW)] == [None, None]
        assert person_2_keypoints.points[get_index_of_keypoint(VoxelKeyPointLabel.LEFT_EAR)] == pytest.approx([
            0.184375, 0.37037037037037035])

    @pytest.mark.unit
    def test_get_classifications(self, metadata_transformer: VoxelMetadataTransformer):
        """Test VoxelMetadataTransformer get_classifications"""

        # GIVEN
        bool_classifications = [
            Classification[bool](
                name="bool_1", value=False), Classification[bool](
                name="bool_2", value=True)]
        float_classifications = [
            Classification[float](
                name="float_1", value=0.4), Classification[float](
                name="float_2", value=0.3)]
        string_classifications = [
            Classification[str](
                name="string_1", value="test"), Classification[str](
                name="string_2", value="test34")]
        int_classifications = [Classification[int](name="int_1", value=2), Classification[int](name="int_2", value=4)]

        object_list = ObjectList(
            bool_attributes=bool_classifications,
            float_attributes=float_classifications,
            string_attributes=string_classifications,
            integer_attributes=int_classifications)
        mf = MediaFrame(
            number=1,
            timestamp=datetime(
                year=2023,
                month=10,
                day=20,
                hour=20,
                tzinfo=pytz.UTC),
            object_list=object_list)

        # WHEN
        classifications = metadata_transformer.get_classifications(mf)
        dict_classifications = classifications.to_dict(True)["classifications"]

        assert_contain_subset_dict(dict_classifications, {"label": "bool_1", "confidence": 0.0})
        assert_contain_subset_dict(dict_classifications, {"label": "bool_2", "confidence": 1.0})
        assert_contain_subset_dict(dict_classifications, {"label": "float_1", "confidence": 0.4})
        assert_contain_subset_dict(dict_classifications, {"label": "float_2", "confidence": 0.3})
        assert_contain_subset_dict(dict_classifications, {"label": "int_1", "confidence": 2.0})
        assert_contain_subset_dict(dict_classifications, {"label": "int_2", "confidence": 4.0})

    @pytest.mark.unit
    def test_transform_snapshot_metadata_to_voxel(self, metadata_transformer: VoxelMetadataTransformer):
        """Test VoxelMetadataTransformer transform_snapshot_metadata_to_voxel"""

        # GIVEN
        keypoints = MagicMock()
        classifications = MagicMock()
        metadata_transformer.get_classifications = Mock(return_value=classifications)
        metadata_transformer.get_pose_keypoints = Mock(return_value=keypoints)
        mocked_metadata = Mock(spec=MediaMetadata)
        mocked_metadata.frames = [Mock()]
        mocked_metadata.resolution = Resolution(width=1920, height=1080)

        # WHEN
        voxel_metadata = metadata_transformer.transform_snapshot_metadata_to_voxel(mocked_metadata)

        # THEN
        metadata_transformer.get_classifications.assert_called_once_with(mocked_metadata.frames[0])
        metadata_transformer.get_pose_keypoints.assert_called_once_with(
            mocked_metadata.frames[0], mocked_metadata.resolution)
        assert voxel_metadata.classifications == classifications
        assert voxel_metadata.keypoints == keypoints

    @pytest.mark.unit
    def test_transform_snapshot_metadata_to_voxel_raises(self, metadata_transformer: VoxelMetadataTransformer):
        """Test VoxelMetadataTransformer transform_snapshot_metadata_to_voxel"""

        # GIVEN
        mocked_metadata = Mock(spec=MediaMetadata)
        mocked_metadata.frames = [Mock()] * 3
        mocked_metadata.resolution = Resolution(width=1920, height=1080)

        # WHEN
        with pytest.raises(VoxelSnapshotMetadataError):
            metadata_transformer.transform_snapshot_metadata_to_voxel(mocked_metadata)

    @pytest.mark.unit
    def test_transform_snapshot_metadata_to_voxel_no_frames(self, metadata_transformer: VoxelMetadataTransformer):
        """Test VoxelMetadataTransformer transform_snapshot_metadata_to_voxel"""

        # GIVEN
        mocked_metadata = Mock(spec=MediaMetadata)
        mocked_metadata.frames = []
        mocked_metadata.resolution = Resolution(width=1920, height=1080)

        # WHEN
        result = metadata_transformer.transform_snapshot_metadata_to_voxel(mocked_metadata)

        # THEN
        assert result == VoxelMetadataFrameFields(fo.Keypoints(keypoints=[]), fo.Classifications(classifications=[]))
