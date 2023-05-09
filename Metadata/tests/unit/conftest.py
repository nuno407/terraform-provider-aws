"""conftest contains common fixtures and mocks for all unit tests"""

import pytest
from unittest.mock import Mock, MagicMock
import sys
from metadata.consumer.voxel.voxel_metadata_kp_mapper import VoxelKPMapper

sys.modules["fiftyone"] = MagicMock()  # noqa
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from metadata.consumer.voxel.constants import KEYPOINTS_SORTED
from base.voxel.constants import CLASSIFICATION_LABEL, BBOX_LABEL, POSE_LABEL
from unittest.mock import Mock, MagicMock
from base.aws.s3 import S3Controller
from mypy_boto3_s3 import S3Client
from metadata.consumer.config import DatasetMappingConfig
from metadata.consumer.voxel.metadata_parser import MetadataParser


def setup_voxel_mocks():
    """
    Setup mocks for voxel.
    """
    voxel_mock: MagicMock = sys.modules["fiftyone"]
    voxel_mock.Keypoint = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Keypoints = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Classification = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Classifications = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Detection = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Detections = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.load_dataset = Mock(return_value=MagicMock())


@pytest.fixture
def fiftyone():
    setup_voxel_mocks()
    return sys.modules["fiftyone"]


@pytest.fixture
def fo_sample() -> dict:
    """
    Used to mock the fiftyone sample.

    Returns:
        MagicMock: _description_
    """
    return {}


@pytest.fixture
def dataset_config() -> DatasetMappingConfig:
    return DatasetMappingConfig(create_dataset_for="datanauts", default_dataset="Debug_Lync", tag="RC")


@pytest.fixture
def s3_client() -> S3Client:
    return MagicMock()


@pytest.fixture
def s3_controller(s3_client: S3Client) -> S3Controller:
    return S3Controller(s3_client)


@pytest.fixture
def metadata_parser() -> MetadataParser:
    return MetadataParser()


@pytest.fixture
def voxel_snapshot_metadata_loader() -> VoxelSnapshotMetadataLoader:
    """
    Class for tests.

    Returns:
        VoxelSnapshotMetadataLoader: _description_
    """
    return VoxelSnapshotMetadataLoader(
        VoxelKPMapper(),
        CLASSIFICATION_LABEL,
        POSE_LABEL,
        BBOX_LABEL)


setup_voxel_mocks()
