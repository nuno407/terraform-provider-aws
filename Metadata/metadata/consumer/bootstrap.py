# pylint: disable=E1120
"""bootstrap dependency injection autowiring."""
import os

import boto3
from kink import di
from metadata.consumer.config import DatasetMappingConfig, MetadataConfig
from mypy_boto3_s3 import S3Client


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""

    di["config_path"] = os.getenv("TENANT_MAPPING_CONFIG_PATH", "/app/config/config.yml")
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    config = MetadataConfig.load_config_from_yaml_file(di["config_path"])
    di[MetadataConfig] = config
    di[DatasetMappingConfig] = config.dataset_mapping
    di[S3Client] = boto3.client("s3", aws_region)
