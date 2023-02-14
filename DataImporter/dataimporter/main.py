"""SDM container script"""
import json
import boto3
import os
from typing import Optional
from dataclasses import dataclass
from base.aws.container_services import ContainerServices
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
from pathlib import Path

CONTAINER_NAME = "DataImporter" # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container
DATA_IMPORTER_QUEUE = os.environ.get("DATA_IMPORTER_QUEUE")

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
    container_services.input_queue = DATA_IMPORTER_QUEUE

    _logger.info("\nListening to input queue(s)..\n\n")

    while stop_condition():
        # Check input SQS queue for new messages
        sqs_messages = container_services.get_multiple_messages_from_input_queue(sqs_client, max_number_of_messages = 100)

        _logger.info("Batch received!")
        for message in sqs_messages:
            # save some messages as examples for development
            _logger.info("Message contents from %s: [%s]", CONTAINER_NAME, message)

            # Processing step
            # processing_sdm(container_services, sqs_client, sqs_message)

            # Delete message after processing
            container_services.delete_message(sqs_client, message['ReceiptHandle'])


if __name__ == '__main__':
    main()
