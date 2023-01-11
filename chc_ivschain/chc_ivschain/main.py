import logging
import os
import time

import boto3
from chc_ivschain.callback_endpoint import CHCCallbackEndpointCreator

from base import ImmediateExit
from base.aws.shared_functions import AWSServiceClients
from base.aws.container_services import ContainerServices
from basehandler.entrypoint import BaseHandler

CONTAINER_NAME = os.getenv("CONTAINER_NAME", "CHC")
CONTAINER_VERSION = 'v1'  # TODO should be removed

AWS_REGION = os.getenv('AWS_REGION', 'eu-central-1')
API_PORT = os.getenv('API_PORT', '5000')
AWS_ENDPOINT = os.getenv('AWS_ENDPOINT', None)
START_DELAY_SECONDS = os.getenv('START_DELAY_SECONDS', "0")
CALLBACK_ENDPOINT = '/cameracheck'

MODE = 'chc'

_logger: logging.Logger = None


def main():
    _logger.info("Starting Container %s...\n", CONTAINER_NAME)
    ImmediateExit()

    _logger.info(f"Start delay {START_DELAY_SECONDS} seconds")
    start_delay = int(START_DELAY_SECONDS)
    time.sleep(start_delay)

    container_services = ContainerServices(CONTAINER_NAME, CONTAINER_VERSION)
    aws_clients = AWSServiceClients(
        s3_client=boto3.client(
            's3', region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT),
        sqs_client=boto3.client(
            'sqs', region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT)
    )
    base_handler = BaseHandler(CONTAINER_NAME,
                               container_services,
                               aws_clients,
                               MODE,
                               CALLBACK_ENDPOINT,
                               CHCCallbackEndpointCreator())
    base_handler.setup_and_run(API_PORT)


if __name__ == '__main__':
    _logger = ContainerServices.configure_logging('chc_ivschain')
    main()
