""" Main Module anon_ivschain """
import logging
import os
import time

import boto3

from base import ImmediateExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import AWSServiceClients
from basehandler.entrypoint import BaseHandler

from anon_ivschain.callback_endpoint import AnonymizeCallbackEndpointCreator
from anon_ivschain.post_processor import AnonymizePostProcessor

CONTAINER_NAME = os.getenv("CONTAINER_NAME", "Anonymize")
CONTAINER_VERSION = "v1"

AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
API_PORT = os.getenv("API_PORT", "5000")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", None)
START_DELAY_SECONDS = os.getenv("START_DELAY_SECONDS", "0")
MOCK_POST_PROCESSOR = bool(os.environ.get("MOCK_POST_PROCESSOR", False))

CALLBACK_ENDPOINT = "/anonymized"

MODE = "anonymize"

_logger: logging.Logger = ContainerServices.configure_logging("anon_ivschain")


def main():
    """
    Main Function
    """

    _logger.info("Starting Container %s...", CONTAINER_NAME)
    ImmediateExit()

    _logger.info("Start delay %s seconds", START_DELAY_SECONDS)
    start_delay = int(START_DELAY_SECONDS)
    time.sleep(start_delay)

    container_services = ContainerServices(CONTAINER_NAME, CONTAINER_VERSION)
    aws_clients = AWSServiceClients(
        s3_client=boto3.client(
            "s3", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT),
        sqs_client=boto3.client(
            "sqs", region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT)
    )

    post_processor = AnonymizePostProcessor(container_services, aws_clients, MOCK_POST_PROCESSOR)
    base_handler = BaseHandler(CONTAINER_NAME,
                               container_services,
                               aws_clients,
                               MODE,
                               CALLBACK_ENDPOINT,
                               AnonymizeCallbackEndpointCreator())

    base_handler.setup_and_run(API_PORT, post_processor)


if __name__ == "__main__":
    main()
