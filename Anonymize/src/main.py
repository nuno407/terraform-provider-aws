"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
import requests

CONTAINER_NAME = "Anonymize"    # Name of the current container
CONTAINER_VERSION = "v5.2"      # Version of the current container


def request_processing_anonymize(client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    and executes the anonymization algorithm (WIP) for the file received and
    updates the relevant info in its relay list

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received
                              and to be sent via message to the input queues of
                              the relevant containers]
    """
    logging.info("Processing message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Download target file to be processed (anonymized)
    raw_file = container_services.download_file(client,
                                                container_services.raw_s3,
                                                dict_body["s3_path"])

    ##########################################################################################
    req_command = 'feature_chain'
    files = [ ('chunk', raw_file)]
    # S3_path MISSING!! -> dict_body["s3_path"]

    ip_pod = '172.20.162.166'
    port_pod = '8080'

    addr = 'http://{}:{}/{}'.format(ip_pod, port_pod, req_command)
    try:
        r = requests.post(addr, files=files)
        logging.info(r)
        status = 0
    except requests.exceptions.ConnectionError as e:
        logging.info(e)
        status = 1

    # ADD EXCEPTION HANDLING IF API NOT AVAILABLE (PUT FILE IN QUEUE?)

    ##########################################################################################
    return dict_body

def update_processing_anonymize(client, container_services, body, pending_dict):
    """Converts the message body to json format (for easier variable access)
    and executes the anonymization algorithm (WIP) for the file received and
    updates the relevant info in its relay list

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received
                              and to be sent via message to the input queues of
                              the relevant containers]
    """
    logging.info("Processing API message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    msg_body = json.loads(new_body)

    dict_body = pending_dict[msg_body['s3_path']]

    # Remove current step/container from the processing_steps
    # list (after processing)
    if dict_body["processing_steps"][0] == CONTAINER_NAME:
        dict_body["processing_steps"].pop(0)

    if dict_body["processing_steps"]:
        # change the current file data_status (if not already changed)
        dict_body["data_status"] = "processing"
    else:
        # change the current file data_status to complete
        # (if current step is the last one from the list)
        dict_body["data_status"] = "complete"

    # Currently just sends the same msg that received
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

    input_sqs_queue = container_services.input_queue
    logging.info("\nListening to %s queue..\n\n", input_sqs_queue)

    # Create pending_queue
    pending_queue = {}

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)
        
        if message:
            # Processing request
            relay_pending = request_processing_anonymize(s3_client,
                                                         container_services,
                                                         message['Body'])
            pending_queue[relay_pending["s3_path"]] = relay_pending

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

        # Check API SQS queue for new update messages
        message_api = container_services.listen_to_input_queue(sqs_client)

        if message_api:
            relay_list = update_processing_anonymize(s3_client,
                                                     container_services,
                                                     message_api['Body'], 
                                                     pending_queue)

            del pending_queue[relay_list["s3_path"]]
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
                                              message_api['ReceiptHandle'])


if __name__ == '__main__':
    main()
