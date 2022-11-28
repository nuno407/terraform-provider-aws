"""Selector container script"""
import boto3
from base.aws.container_services import ContainerServices
from selector.selector import Selector
from selector.footage_api_wrapper import FootageApiWrapper
from selector.footage_api_token_manager import FootageApiTokenManager
from selector.aws_secret_store import AwsSecretStore


CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container


def main():
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client("s3", region_name="eu-central-1")
    sqs_client = boto3.client("sqs", region_name="eu-central-1")

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    aws_secret_store = AwsSecretStore()
    footage_api_secret = aws_secret_store.get_secret(container_services.secret_managers["selector"])
    # Create Api Token Manager
    footage_api_token_manager = FootageApiTokenManager(
        token_endpoint=container_services.api_endpoints["selector_token_endpoint"],
        client_id=footage_api_secret["client_id"],
        client_secret=footage_api_secret["client_secret"]
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
