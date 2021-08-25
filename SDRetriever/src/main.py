"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
import requests

CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container


def processing_request(client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    TODO: COMPLETE DESCRIPTION

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
    """
    logging.info("Processing pipeline message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # TODO: ADD PROCESS LOGIC HERE


def main():
    """Main function"""

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
                                                   CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3',
                             region_name='eu-central-1')
    sqs_client = boto3.client('sqs',
                              region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    logging.info("\nListening to input queue(s)..\n")

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing request
            processing_request(s3_client,
                               container_services,
                               message['Body'])

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])


if __name__ == '__main__':
    main()
