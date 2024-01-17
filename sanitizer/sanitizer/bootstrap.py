# type: ignore
# pylint: disable=E1120
""" bootstrap module. """
import logging
import os
from logging import Logger

import boto3
from kink import di
from mypy_boto3_sns import SNSClient
from pymongo import MongoClient

from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from sanitizer.config import SanitizerConfig


def bootstrap_di():
    """ Initializes dependency injection autowiring. """
    str_level = os.environ.get("LOGLEVEL", "INFO")
    log_level = logging.getLevelName(str_level)
    logging.basicConfig(
        format="%(asctime)s %(name)s\t%(levelname)s\t%(message)s", level=log_level)
    db_uri = os.environ["DB_URI"]
    rcc_aws_endpoint = os.getenv("RCC_AWS_ENDPOINT", None)
    devcloud_aws_endpoint = os.getenv("DEVCLOUD_AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    # useful for local testing
    di["start_delay"] = int(os.getenv("START_DELAY_SECONDS", "0"))
    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    di["container_name"] = os.getenv("CONTAINER_NAME", "Sanitizer")

    config = SanitizerConfig.load_yaml_config(di["config_path"])
    di[SanitizerConfig] = config
    di["default_sns_topic_arn"] = config.topic_arn

    di[Logger] = logging.getLogger("sanitizer")
    di[GracefulExit] = GracefulExit()

    di[MongoClient] = MongoClient(db_uri)
    di["device_info_collection"] = di[MongoClient][config.db_name][config.device_info_collection]

    di[SNSClient] = boto3.client("sns", region_name=aws_region, endpoint_url=devcloud_aws_endpoint)

    rcc_sqs_client = boto3.client("sqs", region_name=aws_region, endpoint_url=rcc_aws_endpoint)
    devcloud_sqs_client = boto3.client("sqs", region_name=aws_region, endpoint_url=devcloud_aws_endpoint)
    di["metadata_sqs_controller"] = SQSController(
        default_sqs_queue_name=config.metadata_queue,
        sqs_client=devcloud_sqs_client)
    di["aws_sqs_rcc_controller"] = SQSController(
        default_sqs_queue_name=config.input_queue,
        sqs_client=rcc_sqs_client)
