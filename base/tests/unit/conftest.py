""" Conftest."""
from base.voxel.models import KeyPointsMapper
from base.testing.mock_functions import set_mock_aws_credentials
from unittest.mock import Mock, MagicMock
from base.aws.s3 import S3Controller
import sys
import pytest
import os

set_mock_aws_credentials()

sys.modules["fiftyone"] = MagicMock()  # noqa

# pylint: disable=missing-function-docstring


@pytest.fixture
def container_services() -> Mock:
    container_service = Mock()
    container_service.upload_file = Mock()
    container_service.raw_s3 = "dev-rcd-raw-video-files"
    container_service.sdr_folder = {"debug": "Debug_Lync/",
                                    "fut2": "FUT2/", "driver_pr": "Driver-PR/"}
    container_service.sqs_queues_list = {
        "SDM": "dev-terraform-queue-s3-sdm",
        "Anonymize": "dev-terraform-queue-anonymize",
        "API_Anonymize": "dev-terraform-queue-api-anonymize",
        "ACAPI": "",
        "CHC": "dev-terraform-queue-chc",
        "API_CHC": "dev-terraform-queue-api-chc",
        "SDRetriever": "dev-terraform-queue-download",
        "Selector": "dev-terraform-queue-selector",
        "Metadata": "dev-terraform-queue-metadata",
        "Output": "dev-terraform-queue-output"
    }
    container_service.RCC_S3_CLIENT = s3_client
    container_service.rcc_info = {"s3_bucket": "rcc-dev-device-data"}
    return container_service


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
def s3_client():
    return Mock()


@pytest.fixture
def s3_controller(s3_client) -> S3Controller:
    return S3Controller(s3_client)


@pytest.fixture
def sqs_client():
    return Mock()


@pytest.fixture
def rcc_bucket():
    return "rcc-prod-device-data"


@pytest.fixture
def rcc_s3_list_prefix():
    return "ridecare_companion_trial/rc_srx_prod_5cd8076d1cbddd483603db282ff9cc00cb76909f/year=2022/month=11/day=01/hour=16/"  # pylint: disable=line-too-long


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


class TestVoxelKPMapper(KeyPointsMapper):
    """
    For testing voxelLoader

    """

    def __init__(self):
        self.__kp_sorted = {
            "LeftAnkle": 0,
            "LeftEar": 1,
            "LeftElbow": 2,
            "LeftEye": 3,
            "LeftHip": 4,
            "LeftKnee": 5,
            "LeftShoulder": 6,
            "LeftWrist": 7,
            "Neck": 8,
            "Nose": 9,
            "RightAnkle": 10,
            "RightEar": 11,
            "RightElbow": 12,
            "RightEye": 13,
            "RightHip": 14,
            "RightKnee": 15,
            "RightShoulder": 16,
            "RightWrist": 17
        }

    def get_keypoint_index(self, name: str) -> int:
        """
        Shall return a desirable position for a keypoint name to be used on the voxel loader.

        Args:
            name (str): Name of the keypoint

        Returns:
            int: The index for the keypoint
        """

        return self.__kp_sorted[name]


setup_voxel_mocks()
