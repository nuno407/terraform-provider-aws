"""conftest contains common fixtures and mocks for all unit tests"""

import pytest
from unittest.mock import Mock, MagicMock
import sys

sys.modules["fiftyone"] = MagicMock()  # noqa
from base.voxel.voxel_snapshot_metadata_loader import VoxelSnapshotMetadataLoader
from metadata.consumer.voxel.constants import KEYPOINTS_SORTED
from base.voxel.constants import CLASSIFICATION_LABEL, BBOX_LABEL, POSE_LABEL
from unittest.mock import Mock, MagicMock
from base.aws.s3 import S3Controller
from mypy_boto3_s3 import S3Client
from metadata.consumer.voxel.metadata_parser import MetadataParser


def setup_voxel_mocks():
    """Setup mocks for voxel"""
    voxel_mock: MagicMock = sys.modules["fiftyone"]
    voxel_mock.Keypoint = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Keypoints = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Classification = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Classifications = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Detection = Mock(side_effect=lambda **kwargs: kwargs)
    voxel_mock.Detections = Mock(side_effect=lambda **kwargs: kwargs)


@pytest.fixture
def fiftyone():
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
        lambda kp: KEYPOINTS_SORTED[kp],
        CLASSIFICATION_LABEL,
        POSE_LABEL,
        BBOX_LABEL)


setup_voxel_mocks()
