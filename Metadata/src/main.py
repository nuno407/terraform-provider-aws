"""Metadata container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
import requests

CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v5.2"     # Version of the current container


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

    logging.info("Processing message..\n")

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
    db_resource = boto3.resource('dynamodb',
                                 region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    input_sqs_queue = container_services.input_queue
    logging.info("\nListening to %s queue..\n\n", input_sqs_queue)

    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)
        '''
        if message:
            # Processing step
            relay_list = processing_metadata(container_services,
                                             message['Body'])

            # Insert/update data in db
            container_services.connect_to_db(db_resource,
                                             relay_list,
                                             message['MessageAttributes'])

            # Send message to output queue of metadata container
            output_queue = container_services.sqs_queues_list["Output"]
            container_services.send_message(sqs_client,
                                            output_queue,
                                            relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])
        '''
        ##########################################################################################
        req_command = 'ready'
        #resource = tmp_file_path
        #files = [ ('chunk', (resource, open(resource, 'rb'),'application/octet-stream'))]

        #payload = {'id': '1'}
        ip_pod = '10.0.18.221'
        port_pod = '5000'

        addr = 'http://{}:{}/{}'.format(ip_pod, port_pod, req_command)
        try:
            r = requests.get(addr)
            logging.info(r)
        except requests.exceptions.ConnectionError as e:
            logging.info(e)

        ##########################################################################################

if __name__ == '__main__':
    main()
