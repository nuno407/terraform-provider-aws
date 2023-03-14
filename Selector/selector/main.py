"""Selector container script"""
import os

import boto3
from selector.footage_api_token_manager import FootageApiTokenManager
from selector.footage_api_wrapper import FootageApiWrapper
from selector.selector import Selector

from base.aws.container_services import ContainerServices

CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container
FOOTAGE_API_CLIENT_ID = os.getenv('FOOTAGE_API_SECRET', None)
FOOTAGE_API_CLIENT_SECRET = os.getenv('FOOTAGE_API_SECRET', None)
class SecretMissingError(Exception):
    """ Raised when footage api secret is not provided. """

def main():
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client("sqs", region_name="eu-central-1")

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars()

    if FOOTAGE_API_CLIENT_ID is None:
        raise SecretMissingError("FOOTAGE_API_CLIENT_ID must be provided")
    if FOOTAGE_API_CLIENT_SECRET is None:
        raise SecretMissingError("FOOTAGE_API_CLIENT_SECRET must be provided")

    # Create Api Token Manager
    footage_api_token_manager = FootageApiTokenManager(
        token_endpoint=container_services.api_endpoints["selector_token_endpoint"],
        client_id=FOOTAGE_API_CLIENT_ID,
        client_secret=FOOTAGE_API_CLIENT_SECRET
    )

    footage_api_wrapper = FootageApiWrapper(
        footage_api_url=container_services.api_endpoints["mdl_footage_endpoint"],
        footage_api_token_manager=footage_api_token_manager)

    # Initialize Application class
    selector = Selector(
        sqs_client=sqs_client,
        container_services=container_services,
        footage_api_wrapper=footage_api_wrapper,
        hq_queue_name=container_services.sqs_queues_list["HQ_Selector"]
    )

    # Main loop
    while True:
        # selector.handle_selector_queue()
        selector.handle_hq_queue()


if __name__ == "__main__":
    _logger = ContainerServices.configure_logging("selector")
    main()
