""" bootstrap module. """
import logging
import os

import boto3
from kink import di
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from pymongo import MongoClient
from logging import Logger

from sanitizer.artifact.filter import ArtifactFilter
from sanitizer.artifact.forwarder import ArtifactForwarder
from sanitizer.artifact.parser import ArtifactParser
from sanitizer.artifact.persistence import ArtifactPersistence
from sanitizer.aws.sns import AWSSNSController
from sanitizer.aws.sqs import AWSSQSController
from sanitizer.message.filter import MessageFilter
from sanitizer.message.parser import MessageParser
from sanitizer.message.persistence import MessagePersistence


def bootstrap_di():
    """ Initializes dependency injection autowiring. """
    aws_endpoint = os.getenv("AWS_ENDPOINT", None)
    aws_region = os.getenv("AWS_REGION", "eu-central-1")

    di[Logger] = logging.getLogger("sanitizer")

    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    di["tenant_config_path"] = os.getenv("TENANT_CONFIG_PATH", "/app/config/config.yml")
    db_uri = os.getenv("DB_URI", None)
    di[MongoClient] = MongoClient(db_uri)

    di[SQSClient] = boto3.client("sqs", region_name=aws_region, endpoint_url=aws_endpoint)
    di[SNSClient] = boto3.client("sns", region_name=aws_region, endpoint_url=aws_endpoint)

    di[AWSSQSController] = AWSSQSController()
    di[AWSSNSController] = AWSSNSController()

    di[MessageParser] = MessageParser()
    di[MessageFilter] = MessageFilter()
    di[MessagePersistence] = MessagePersistence()

    di[ArtifactParser] = ArtifactParser()
    di[ArtifactFilter] = ArtifactFilter()
    di[ArtifactPersistence] = ArtifactPersistence()
    di[ArtifactForwarder] = ArtifactForwarder()
