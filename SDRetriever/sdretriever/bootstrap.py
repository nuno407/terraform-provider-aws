"""bootstrap dependency injection autowiring"""
import os

import boto3
from kink import di
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sts import STSClient

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller, S3ControllerFactory
from base.aws.shared_functions import StsHelper
from base.graceful_exit import GracefulExit
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import CONTAINER_NAME, CONTAINER_VERSION
from sdretriever.s3_finder import S3Finder


def __create_rcc_s3_boto3_client(sts_helper: StsHelper) -> S3ClientFactory:
    def factory() -> S3Client:
        credentials = sts_helper.get_credentials()
        return boto3.client(
            "s3",
            region_name="eu-central-1",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"])
    return factory


def __create_rcc_s3_controller(s3_client_factory: S3ClientFactory) -> S3ControllerFactory:
    def factory() -> S3Controller:
        return S3Controller(s3_client_factory())
    return factory


def bootstrap_di():
    """ Initializes dependency injection autowiring. """
    # configmap
    di["config_path"] = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    config = SDRetrieverConfig.load_config_from_yaml_file(di["config_path"])
    di[SDRetrieverConfig] = config

    # string constants
    di["default_sqs_queue_name"] = config.input_queue

    # boto3 clients
    di[SQSClient] = boto3.client("sqs", region_name="eu-central-1")
    di[S3Client] = boto3.client("s3", region_name="eu-central-1")
    di[STSClient] = boto3.client("sts", region_name="eu-central-1")

    # base aws services
    di[ContainerServices] = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    di[ContainerServices].load_config_vars()
    di[StsHelper] = StsHelper(
        di[STSClient],
        role=di[ContainerServices].rcc_info.get("role"),
        role_session="DevCloud-SDRetriever")

    # rcc boto3 clients
    di[S3ClientFactory] = __create_rcc_s3_boto3_client(di[StsHelper])
    di[S3ControllerFactory] = __create_rcc_s3_controller(di[S3ClientFactory])

    # graceful exit
    di[GracefulExit] = GracefulExit()
    di[S3Finder] = S3Finder()

    # configure logging
    di[ContainerServices].configure_logging("SDRetriever")
