import boto3
import json
import logging
from datetime import datetime
import pytz
from baseaws.shared_functions import ContainerServices

CONTAINER_NAME = "SDM"          # Name of the current container
CONTAINER_VERSION = "v5.1"      # Version of the current container


def processing_sdm(container_services, body):
    """Retrieves the MSP name from the message received and creates
    a relay list for the current file

    Arguments:
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from
                          the received message]
    Returns:
        relay_data {dict} -- [dict with the relevant info for the file received
                            and to be sent via message to the input queues
                            of the relevant containers]
    """

    logging.info("Processing message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Access key value from msg body
    key_value = dict_body["Records"][0]["s3"]["object"]["key"]
    msp = key_value.split('/')[0]

    # Creates relay list to be used by other containers
    relay_data = {}
    relay_data["processing_steps"] = container_services.sdm_processing_list[msp]
    relay_data["s3_path"] = key_value
    relay_data["data_status"] = "received"

    timestamp = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    logging.info("\nProcessing complete!")
    logging.info("    -> key: {}".format(relay_data["s3_path"]))
    logging.info("    -> timestamp: {}\n".format(timestamp))

    return relay_data


def main():
    """Main function"""

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s',
                        level=logging.INFO)

    logging.info("Starting Container {} ({})\n".format(CONTAINER_NAME,
                                                       CONTAINER_VERSION))

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

    logging.info("\nListening to {} queue..\n\n".format(container_services.input_queue))

    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing step
            relay_list = processing_sdm(container_services, message['Body'])

            # Send message to input queue of the next processing step
            # (if applicable)
            if relay_list["processing_steps"]:
                container_services.send_message(sqs_client,
                                                container_services.output_queues_list[relay_list["processing_steps"][0]],
                                                relay_list)

            # Send message to input queue of metadata container
            container_services.send_message(sqs_client,
                                            container_services.output_queues_list["Metadata"],
                                            relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

if __name__ == '__main__':
    main()
