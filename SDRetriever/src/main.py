"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
from datetime import datetime

CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container


def processing_request(s3_client, kinesis_client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    TODO: COMPLETE DESCRIPTION

    Arguments:
        s3_client {boto3.client} -- [client used to access
                                     the S3 service]
        kinesis_client {boto3.client} -- [client used to access
                                          the Kinesis service]
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

    # TODO: CONVERT MSG PARAMETERS TO BE USED ON GET_KINESIS_CLIP FUNCTION

    # TEST VALUES 
    stream_name = 'test-kinesis'
    start_time = datetime(2021, 8, 29)
    end_time = datetime(2021, 8, 30)
    selector = 'PRODUCER_TIMESTAMP'  # 'PRODUCER_TIMESTAMP'|'SERVER_TIMESTAMP'

    # Get Kinesis clip using received message parameters
    video_clip = container_services.get_kinesis_clip(kinesis_client,
                                                     stream_name,
                                                     start_time,
                                                     end_time,
                                                     selector)

    # TODO: DEFINE S3 PATH FOR CLIP
    s3_path = 'lyft/test_clip.mp4'

    # Upload video clip into raw data S3 bucket
    container_services.upload_file(s3_client,
                                   video_clip,
                                    container_services.raw_s3,
                                    s3_path)

    # TODO: ADD CONCATENATION SCRIPT HERE

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
    kinesis_client = boto3.client('kinesisvideo',
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
                               kinesis_client,
                               container_services,
                               message['Body'])

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])


if __name__ == '__main__':
    main()
