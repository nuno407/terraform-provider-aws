""" selector bootstrap module. """
import os

import boto3
from kink import di
from mypy_boto3_sqs import SQSClient

from base.aws.container_services import ContainerServices
from base.graceful_exit import GracefulExit
from selector.constants import CONTAINER_NAME, CONTAINER_VERSION


class SecretMissingError(Exception):
    """ Raised when footage api secret is not provided. """


def bootstrap_di():
    """ Bootstrap the dependency injection container. """
    # Create the necessary clients for AWS services access
    di[SQSClient] = boto3.client("sqs", region_name="eu-central-1")

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars()
    di[ContainerServices] = container_services
    di[GracefulExit] = GracefulExit()

    footage_api_client_id = os.getenv("FOOTAGE_API_CLIENT_ID", None)
    if footage_api_client_id is None:
        raise SecretMissingError("FOOTAGE_API_CLIENT_ID must be provided")

    footage_api_client_secret = os.getenv("FOOTAGE_API_CLIENT_SECRET", None)
    if footage_api_client_secret is None:
        raise SecretMissingError("FOOTAGE_API_CLIENT_SECRET must be provided")

    di["client_id"] = footage_api_client_id
    di["client_secret"] = footage_api_client_secret
    di["token_endpoint"] = container_services.api_endpoints["selector_token_endpoint"]
    di["footage_api_url"] = container_services.api_endpoints["mdl_footage_endpoint"]
    di["default_sqs_queue_name"] = container_services.sqs_queues_list["HQ_Selector"]
