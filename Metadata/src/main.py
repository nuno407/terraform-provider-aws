"""Metadata container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices

CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v6.2"     # Version of the current container


def processing_metadata(container_services, body):
    """Copies the relay list info received from other containers and
    converts it from string into a dictionary

    Arguments:
        container_services {BaseAws.shared_functions.ContainerServices}
                        -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from
                          the received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received
                            and to be sent via message to the output queue]
    """

    logging.info("Processing pipeline message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # currently just sends the same msg that received
    relay_data = dict_body

    container_services.display_processed_msg(relay_data["s3_path"])

    return relay_data


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

    logging.info("\nListening to input queue(s)..\n\n")

    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing step
            relay_list = processing_metadata(container_services,
                                             message['Body'])

            # Insert/update data in db
            container_services.connect_to_docdb(relay_list,
                                             message['MessageAttributes'])

            # Send message to output queue of metadata container
            output_queue = container_services.sqs_queues_list["Output"]
            container_services.send_message(sqs_client,
                                            output_queue,
                                            relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

if __name__ == '__main__':
    main()
