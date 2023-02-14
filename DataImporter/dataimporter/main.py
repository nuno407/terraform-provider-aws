"""SDM container script"""
import json
import boto3
from typing import Optional
from dataclasses import dataclass
from base.aws.container_services import ContainerServices
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
from pathlib import Path

CONTAINER_NAME = "DataImporter" # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container

_logger = ContainerServices.configure_logging('dataimporter')

def main(stop_condition=lambda: True):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3', region_name='eu-central-1')
    sqs_client = boto3.client('sqs', region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    _logger.info("\nListening to input queue(s)..\n\n")

    while stop_condition():
        # Check input SQS queue for new messages
        sqs_message = container_services.get_single_message_from_input_queue(sqs_client)

        if sqs_message:
            # save some messages as examples for development
            _logger.info("Message contents from %s: [%s]", CONTAINER_NAME, sqs_message)

            # Processing step
            # processing_sdm(container_services, sqs_client, sqs_message)

            # Delete message after processing
            container_services.delete_message(sqs_client, sqs_message['ReceiptHandle'])


if __name__ == '__main__':
    main()
