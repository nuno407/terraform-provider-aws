# type: ignore
# pylint: disable=E1120
""" bootstrap module. """
import logging
import os
from logging import Logger

import boto3
from kink import di
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from pymongo import MongoClient

from base.graceful_exit import GracefulExit
from sanitizer.artifact.artifact_injector import MetadataArtifactInjector
from sanitizer.config import SanitizerConfig


def bootstrap_di():
    """ Initializes dependency injection autowiring. """
    str_level = os.environ.get("LOGLEVEL", "INFO")
    log_level = logging.getLevelName(str_level)
    logging.basicConfig(
        format="%(asctime)s %(name)s\t%(levelname)s\t%(message)s", level=log_level)
    db_uri = os.environ["DB_URI"]
    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    # useful for local testing
    di["start_delay"] = int(os.getenv("START_DELAY_SECONDS", "0"))
    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")

    config = SanitizerConfig.load_yaml_config(di["config_path"])
    di[SanitizerConfig] = config
    di["default_sqs_queue_name"] = config.input_queue
    di["default_sns_topic_arn"] = config.topic_arn

    di[Logger] = logging.getLogger("sanitizer")
    di[GracefulExit] = GracefulExit()

    di[MongoClient] = MongoClient(db_uri)

    di[SQSClient] = boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint)
    di[SNSClient] = boto3.client("sns", region_name=aws_region, endpoint_url=aws_endpoint)
    di[MetadataArtifactInjector] = MetadataArtifactInjector()
