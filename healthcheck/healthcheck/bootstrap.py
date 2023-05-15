# type: ignore
# pylint: disable=E1120
"""bootstrap dependency injection autowiring."""
import logging
import os
from dataclasses import dataclass
from logging import Logger
from typing import Optional

import boto3
from kink import di
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from pymongo import MongoClient

from base.model.artifacts import RecorderType
from base.graceful_exit import GracefulExit
from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.checker.snapshot import SnapshotArtifactChecker
from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker
from healthcheck.config import HealthcheckConfig
from healthcheck.tenant_config import DatasetMappingConfig, TenantConfig
from healthcheck.database import INoSQLDBClient, NoSQLDBConfiguration
from healthcheck.model import S3Params
from healthcheck.mongo import MongoDBClient
from healthcheck.schema.validator import DocumentValidator, JSONSchemaValidator
from healthcheck.voxel_client import VoxelClient, VoxelEntriesGetter


@dataclass
class EnvironmentParams:
    """Environment parameters."""
    aws_endpoint: str
    aws_region: str
    container_version: str
    config_path: str
    tenant_config_path: str
    db_uri: str
    webhook_url: Optional[str]


def get_environment() -> EnvironmentParams:
    """Returns environment parameters."""
    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")
    container_version = os.getenv("CONTAINER_VERSION", "development")
    config_path = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    tenant_config_path = os.getenv("TENANT_CONFIG_PATH", "/app/config/config.yml")
    db_uri = os.getenv("FIFTYONE_DATABASE_URI")
    webhook_url = os.getenv("MSTEAMS_WEBHOOK", "")

    return EnvironmentParams(
        aws_endpoint=aws_endpoint,
        aws_region=aws_region,
        container_version=container_version,
        config_path=config_path,
        tenant_config_path=tenant_config_path,
        db_uri=db_uri,
        webhook_url=webhook_url
    )


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""
    env = get_environment()
    di[Logger] = logging.getLogger("healthcheck")

    di["container_version"] = env.container_version
    di["config_path"] = env.config_path
    di["db_uri"] = env.db_uri
    di["webhook_url"] = env.webhook_url
    di["tenant_config_path"] = env.tenant_config_path

    di[SQSClient] = boto3.client(
        service_name="sqs",
        region_name=env.aws_region,
        endpoint_url=env.aws_endpoint)
    di[S3Client] = boto3.client(
        service_name="s3",
        region_name=env.aws_region,
        endpoint_url=env.aws_endpoint)
    di[GracefulExit] = GracefulExit()

    config = HealthcheckConfig.load_yaml_config(di["config_path"])
    di[HealthcheckConfig] = config
    tenant_config = TenantConfig.load_config_from_yaml_file(di["tenant_config_path"])
    di[DatasetMappingConfig] = tenant_config.dataset_mapping
    di["default_sqs_queue_name"] = config.input_queue

    di[S3Params] = S3Params(
        config.anonymized_s3_bucket,
        config.raw_s3_bucket)

    di[NoSQLDBConfiguration] = lambda _di: NoSQLDBConfiguration(
        _di[HealthcheckConfig].db_name,
        _di[HealthcheckConfig].environment_prefix,
        _di[HealthcheckConfig])

    di[MongoClient] = lambda _di: MongoClient(_di["db_uri"])

    di[DocumentValidator] = JSONSchemaValidator()
    di[VoxelEntriesGetter] = VoxelClient()
    di[INoSQLDBClient] = MongoDBClient()

    di["checkers"] = {
        RecorderType.TRAINING: TrainingRecorderArtifactChecker(),
        RecorderType.INTERIOR: InteriorRecorderArtifactChecker(),
        RecorderType.SNAPSHOT: SnapshotArtifactChecker()
    }
