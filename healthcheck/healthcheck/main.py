import logging
import os
from dataclasses import dataclass
from datetime import datetime
from healthcheck.healthcheck.rcc_artifact_checker import RCCArtifact
from base.aws.shared_functions import AWSServiceClients
from base.aws.container_services import ContainerServices

CONTAINER_NAME = os.getenv("CONTAINER_NAME", "CHC")
AWS_REGION = os.getenv('AWS_REGION', 'eu-central-1')

LOGGER: logging.Logger = None

VIDEO_TYPE = 1
IMAGE_TYPE = 2


def main() -> None:
    LOGGER.info("Starting Container %s...\n", CONTAINER_NAME)

    #container_services = ContainerServices(CONTAINER_NAME, CONTAINER_VERSION)
    #aws_clients = AWSServiceClients(
    #    s3_client=boto3.client(
    #        's3', region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT),
    #    sqs_client=boto3.client(
    #        'sqs', region_name=AWS_REGION, endpoint_url=AWS_ENDPOINT)
    #)
    #base_handler = BaseHandler(CONTAINER_NAME,
    #                           container_services,
    #                           aws_clients,
    #                           MODE,
    #                           CALLBACK_ENDPOINT,
    #                           CHCCallbackEndpointCreator())
    #base_handler.setup_and_run(API_PORT)


if __name__ == '__main__':
    _logger = ContainerServices.configure_logging('chc_ivschain')
    main()
