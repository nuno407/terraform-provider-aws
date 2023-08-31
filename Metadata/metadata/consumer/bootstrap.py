# pylint: disable=E1120
"""bootstrap dependency injection autowiring."""
import os

import boto3
from kink import di
from metadata.consumer.config import MetadataConfig
from mypy_boto3_s3 import S3Client
from metadata.consumer.voxel.metadata_parser import MetadataParser
from metadata.consumer.voxel.voxel_metadata_kp_mapper import VoxelKPMapper
from metadata.consumer.imu_gap_finder import IMUGapFinder
from base.voxel.constants import POSE_LABEL, BBOX_LABEL, CLASSIFICATION_LABEL
from base.voxel.models import KeyPointsMapper
from base.aws.s3 import S3Controller


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""

    di["config_path"] = os.getenv(
        "TENANT_MAPPING_CONFIG_PATH", "/app/config/config.yml")
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    di["pose_label"] = POSE_LABEL
    di["bbox_label"] = BBOX_LABEL
    di["classification_label"] = CLASSIFICATION_LABEL

    di[KeyPointsMapper] = VoxelKPMapper()
    di[MetadataConfig] = MetadataConfig.load_yaml_config(di["config_path"])
    di[MetadataParser] = MetadataParser()
    di[S3Client] = boto3.client("s3", aws_region)
    di[S3Controller] = S3Controller()
    di[IMUGapFinder] = IMUGapFinder()
