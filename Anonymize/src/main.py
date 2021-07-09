"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
import requests

CONTAINER_NAME = "Anonymize"    # Name of the current container
CONTAINER_VERSION = "v5.2"      # Version of the current container


def processing_anonymize(client, container_services, body):
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

    # INSERT ANONYMIZATION ALGORITHM HERE

    # Upload processed file
    container_services.upload_file(client,
                                   raw_file,
                                   container_services.anonymized_s3,
                                   dict_body["s3_path"])

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

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)
        '''
        if message:
            # Processing step
            relay_list = processing_anonymize(s3_client,
                                              container_services,
                                              message['Body'])

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
        '''
        req_command = 'feature_chain'
        #resource = tmp_file_path
        #files = [ ('chunk', (resource, open(resource, 'rb'),'application/octet-stream'))]
        raw_file = container_services.download_file(s3_client,
                                                    container_services.raw_s3,
                                                    "lync/Hanau02_Passat_625_windshield_top_nir_merged_ros.mp4")

        files = [ ('chunk', raw_file)]
        payload = {'id': '1'}
        ip_pod = '172.20.89.71'
        port_pod = '80'

        addr = 'http://{}:{}/{}'.format(ip_pod, port_pod, req_command)
        r = requests.post(addr, files=files, data=payload)
        ##########################################################################################

if __name__ == '__main__':
    main()
