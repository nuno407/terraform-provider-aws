# pylint: disable=E1120
""" bootstrap module. """
import os

import boto3
from kink import di
from mypy_boto3_sqs import SQSClient
from mypy_boto3_s3 import S3Client

from base.graceful_exit import GracefulExit
from base.aws.container_services import ContainerServices
from artifact_downloader.config import ArtifactDownloaderConfig
from artifact_downloader.chc_synchronizer import ChcSynchronizer


def bootstrap_di():
    """ Initializes dependency injection autowiring. """

    ContainerServices.configure_logging("artifact_downloader")

    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    di["container_name"] = os.getenv("CONTAINER_NAME", "ArtifactDownloader")

    config = ArtifactDownloaderConfig.load_yaml_config(di["config_path"])
    di[ArtifactDownloaderConfig] = config
    di["default_sqs_queue_name"] = config.input_queue
    di[ChcSynchronizer] = ChcSynchronizer()

    di[GracefulExit] = GracefulExit()

    di[SQSClient] = boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint)
    di[S3Client] = boto3.client("s3", region_name=aws_region, endpoint_url=aws_endpoint)
