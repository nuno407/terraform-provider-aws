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

from base.graceful_exit import GracefulExit
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.checker.snapshot import SnapshotArtifactChecker
from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker
from healthcheck.config import HealthcheckConfig
from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.aws_sqs import SQSMessageController
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.database import INoSQLDBClient, NoSQLDBConfiguration
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import ArtifactType, S3Params
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
    db_uri: str
    webhook_url: Optional[str]


def get_environment() -> EnvironmentParams:
    """Returns environment parameters."""
    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")
    container_version = os.getenv("CONTAINER_VERSION", "development")
    config_path = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    db_uri = os.getenv("FIFTYONE_DATABASE_URI")
    webhook_url = os.getenv("MSTEAMS_WEBHOOK", "")

    return EnvironmentParams(
        aws_endpoint=aws_endpoint,
        aws_region=aws_region,
        container_version=container_version,
        config_path=config_path,
        db_uri=db_uri,
        webhook_url=webhook_url
    )


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""
    env_params = get_environment()
    di[Logger] = logging.getLogger("healthcheck")

    di["container_version"] = env_params.container_version
    di["config_path"] = env_params.config_path
    di["db_uri"] = env_params.db_uri
    di["webhook_url"] = env_params.webhook_url

    di[SQSClient] = boto3.client(
        service_name="sqs",
        region_name=env_params.aws_region,
        endpoint_url=env_params.aws_endpoint)
    di[S3Client] = boto3.client(
        service_name="s3",
        region_name=env_params.aws_region,
        endpoint_url=env_params.aws_endpoint)
    di[GracefulExit] = GracefulExit()

    config = HealthcheckConfig.load_yaml_config(di["config_path"])
    di[HealthcheckConfig] = config

    di[S3Params] = S3Params(
        config.anonymized_s3_bucket,
        config.raw_s3_bucket,
        config.s3_dir)

    di[NoSQLDBConfiguration] = lambda _di: NoSQLDBConfiguration(
        _di[HealthcheckConfig].db_name,
        _di[HealthcheckConfig].environment_prefix,
        _di[HealthcheckConfig])

    di[MongoClient] = lambda _di: MongoClient(_di["db_uri"])
    di[SQSMessageParser] = SQSMessageParser()
    di[DocumentValidator] = JSONSchemaValidator()
    di[VoxelEntriesGetter] = VoxelClient()
    di[INoSQLDBClient] = MongoDBClient()
    di[ArtifactParser] = ArtifactParser()
    di[S3Controller] = S3Controller()
    di[SQSMessageController] = SQSMessageController()
    di[DatabaseController] = DatabaseController()
    di[VoxelFiftyOneController] = VoxelFiftyOneController()
    di["checkers"] = {
        ArtifactType.TRAINING_RECORDER: TrainingRecorderArtifactChecker(),
        ArtifactType.INTERIOR_RECORDER: InteriorRecorderArtifactChecker(),
        ArtifactType.SNAPSHOT: SnapshotArtifactChecker()
    }
