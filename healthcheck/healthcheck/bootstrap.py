"""bootstrap dependency injection autowiring."""
import os

import boto3
from kink import di
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from pymongo import MongoClient

from base.graceful_exit import GracefulExit
from healthcheck.config import HealthcheckConfig
from healthcheck.database import DBConfiguration
from healthcheck.message_parser import SQSMessageParser
from healthcheck.model import S3Params
from healthcheck.schema.validator import DocumentValidator, JSONSchemaValidator
from healthcheck.voxel_client import VoxelEntriesGetter, VoxelClient
from healthcheck.checker.training_recorder import TrainingRecorderArtifactChecker
from healthcheck.checker.interior_recorder import InteriorRecorderArtifactChecker
from healthcheck.checker.snapshot import SnapshotArtifactChecker
from healthcheck.database import DBClient
from healthcheck.mongo import MongoDBClient
from healthcheck.model import ArtifactType
from healthcheck.artifact_parser import ArtifactParser
from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.controller.aws_sqs import SQSMessageController


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""
    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    di["container_version"] = os.getenv("CONTAINER_VERSION", "development")

    aws_region = os.getenv("AWS_REGION", "eu-central-1")
    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yaml")
    di["db_uri"] = os.getenv("DB_URI")

    di[SQSClient] = lambda _di: boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint)
    di[S3Client] = lambda _di: boto3.client("s3", region_name=aws_region, endpoint_url=aws_endpoint)
    di[GracefulExit] = GracefulExit()

    config = HealthcheckConfig.load_yaml_config(di["config_path"])
    di[HealthcheckConfig] = config

    di[S3Params] = S3Params(
        config.anonymized_s3_bucket,
        config.raw_s3_bucket,
        config.s3_dir)

    di[DBConfiguration] = lambda _di: DBConfiguration(
        _di[HealthcheckConfig].db_name,
        _di[HealthcheckConfig].environment_prefix,
        _di[HealthcheckConfig])

    di[MongoClient] = lambda _di: MongoClient(_di["db_uri"])
    di[SQSMessageParser] = SQSMessageParser()
    di[DocumentValidator] = JSONSchemaValidator()
    di[VoxelEntriesGetter] = VoxelClient()
    di[DBClient] = MongoDBClient()
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
