"""Selector container script"""
import json
import logging
import uuid
import boto3
from baseaws.shared_functions import ContainerServices
import requests
from datetime import datetime, timedelta

CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container


def request_process_selector(client, container_services, body, pending_list):
    """Converts the message body to json format (for easier variable access)
    and sends an API request for the ivs feature chain container with the
    file downloaded to be processed.

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
        pending_list {dict} -- [dictionary containing all the pending
                                processing requests (identified by an
                                uuid and their respective relay_lists)]
    """
    logging.info("Processing pipeline message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Add entry for current video relay list on pending queue
    pending_list[uid] = dict_body

    # Picking Device Id from header
    device_id = dict_body.get("header").get('device_id')

    for info in dict_body.get("recording_info"):

        print(info.get("recording_state"))
        if info.get('events'):
            for event in info.get('events'):
                if event.get("value", "") == '1':
                    # Create a random uuid to identify a given camera health check process
                    uid = str(uuid.uuid4())
                    payload = {'device_id': device_id}
                    timestamps = event.get('timestamp_ms')
                    cal_date = datetime.fromtimestamp(int(timestamps[:10]))
                    # print(cal_date, timestamps)

                    prev_timestamps = int(datetime.timestamp(cal_date - timedelta(seconds=5)))
                    post_timestamps = int(datetime.timestamp(cal_date + timedelta(seconds=5)))

                    payload.update({'uid': uid, 'start_time': str(prev_timestamps), 'end_time': str(post_timestamps)})



                    # Send API request (POST)
                    try:
                        requests.post(addr, files=files, data=payload)
                        logging.info("API POST request sent! (uid: %s)", uid)
                    except requests.exceptions.ConnectionError as error_response:
                        logging.info(error_response)

    

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

    # Define additional input SQS queues to listen to
    # (container_services.input_queue is the default queue
    # and doesn't need to be declared here)
    # api_sqs_queue = container_services.sqs_queues_list['API_CHC']

    # logging.info("\nListening to input queue(s)..\n")

    # Create pending_queue
    # Entries format: {'<uid>': <relay_list>}
    pending_queue = {}

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing request
            request_process_selector(s3_client,
                                         container_services,
                                         message['Body'],
                                         pending_queue)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

        

if __name__ == '__main__':
    main()
