"""SDM container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
import os

CONTAINER_NAME = "SDM"          # Name of the current container
CONTAINER_VERSION = "v6.2"      # Version of the current container


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

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Access key value from msg body
    key_value = dict_body["Records"][0]["s3"]["object"]["key"]
    # Get provider (MSP) name
    msp = key_value.split('/')[0]

    # If file is stored outside the providers folders then
    # provide a warning about the error and stop processing the current file
    try:
        key_value.split('/')[1]
    except:
        logging.info("\nWARNING: File %s will not be processed!!", msp)
        logging.info("Reason: File is outside MSP folders\n")
        relay_data = {}
        return relay_data

    # If received file format is in the config file ignore list,
    # then the processing of the file is stopped and the file is
    # ignored by the data ingestion pipeline
    file_name = key_value.split('/')[-1]
    file_format = file_name.split('.')[-1]
    if file_format in container_services.raw_s3_ignore:
        logging.info("\nWARNING: File %s will not be processed!!", key_value)
        logging.info("Reason: File format is on the Raw Data S3 ignore list\n")
        relay_data = {}
        return relay_data

    # TODO: DEFINE PROCESS FOR METADATA FULL FILES (SHOULD BE DIRECTLY ADDED TO DB?)

    ################################################################################################### DEBUG SDRETRIEVER PROCESSING (REMOVE AFTERWARDS)
    # if "TEST_TENANT" in file_name:
    #     logging.info("\nWARNING: File %s will not be processed!!", key_value)
    #     logging.info("Reason: Tenant is on the Raw Data S3 ignore list\n")
    #     relay_data = {}
    #     return relay_data
    ###################################################################################################

    logging.info("Processing pipeline message..\n")

    # Creates relay list to be used by other containers
    relay_data = {}
    relay_data["processing_steps"] = container_services.msp_steps[msp]
    relay_data["s3_path"] = key_value
    relay_data["data_status"] = "received"

    container_services.display_processed_msg(relay_data["s3_path"])

    return relay_data

def log_message(message, queue=CONTAINER_NAME):
    logging.info("\n######################################\n")
    logging.info("Message contents from %s:\n"%(queue))
    logging.info(message)
    logging.info("\n######################################\n")

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
            # save some messages as examples for development
            log_message(message)
            # Processing step
            relay_list = processing_sdm(container_services, message['Body'])

            # If file received is valid
            if relay_list:

                # Send message to input queue of the next processing step
                # (if applicable)
                if relay_list["processing_steps"]:
                    next_step = relay_list["processing_steps"][0]
                    next_queue = container_services.sqs_queues_list[next_step]
                    container_services.send_message(sqs_client,
                                                    next_queue,
                                                    relay_list)

                # Send message to input queue of metadata container
                metadata_queue = container_services.sqs_queues_list["Metadata"]
                container_services.send_message(sqs_client,
                                                metadata_queue,
                                                relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])


if __name__ == '__main__':
    main()
