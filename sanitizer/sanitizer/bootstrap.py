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

from base.aws.sns import SNSController
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from sanitizer.config import SanitizerConfig

def bootstrap_di():
    """ Initializes dependency injection autowiring. """
    db_uri = os.getenv("DB_URI", None)

    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")

    config = SanitizerConfig.load_yaml_config(di["config_path"])
    di[SanitizerConfig] = config
    di["default_sns_topic_arn"] = config.topic_arn # bound with kink DI

    di[Logger] = logging.getLogger("sanitizer")
    di[GracefulExit] = GracefulExit()

    di[MongoClient] = MongoClient(db_uri)

    di[SQSClient] = boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint)
    di[SNSClient] = boto3.client("sns", region_name=aws_region, endpoint_url=aws_endpoint)

    di[SQSController] = SQSController(config.input_queue, di[SQSClient])
    di[SNSController] = SNSController(di[SNSClient])
