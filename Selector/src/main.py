"""Selector container script"""
import json
import logging
import uuid
import boto3
from baseaws.shared_functions import ContainerServices
import requests
from datetime import datetime, timedelta
import os
import base64
import urllib3
from json.decoder import JSONDecodeError
from urllib.parse import urlencode
import aws_secrets_manager.aws_secrets_manager as aws_secrets_manager


http_client = urllib3.PoolManager()

# secret = os.environ.get('RCC_API_DEV_SECRET_NAME')
secret = 'API_DEVCLOUD_SELECTOR_SECRET'
aws_secrets = aws_secrets_manager.get_secret(secret)


CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container

#####  Generating Token for API Authorization ###########

def get_token(token_endpoint, client_id, client_secret, scopes) -> dict:
    client_auth = base64.b64encode((client_id + ':' + client_secret).encode('utf-8')).decode('utf-8')

    headers = {}
    headers['Content-Type'] = 'application/x-www-form-urlencoded'
    headers['Authorization'] = 'Basic ' + client_auth

    body = {
        'grant_type': 'client_credentials',
        'scope': scopes
    }
    encoded_body = urlencode(body)

    try:
        response = http_client.request('POST', token_endpoint, headers=headers, body=encoded_body)
        #requests.post(token_endpoint, headers=headers, data=encoded_body)

        if response.status == 200:
            json_response = json.loads(response.data.decode('utf-8'))
            return json_response
        else:
            print("Error getting access token, status: ", response.status, ", cause: ", response.data)
            return None
    except JSONDecodeError:
        print("String could not be converted to JSON")
        return None

def refresh_api_token() -> dict:
        current_timestamp_s = int(datetime.now().timestamp())

        token = get_token(
            aws_secrets['oauthTokenEndpoint'],
            aws_secrets['clientId'],
            aws_secrets['clientSecret'],
            aws_secrets['oauthScopes'])

        # Substract 5 minutes from the expiration date to avoid expired tokens due to processing time, network delay, etc.
        # 5 minutes is a random chosen value.
        token['expiration_timestamp_s'] = current_timestamp_s + token.get('expires_in') - (5 * 60)
        return token



def request_process_selector(client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    and sends an API request for the ivs feature chain container with the
    file downloaded to be processed.

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

    # Add entry for current video relay list on pending queue
    #pending_list[uid] = dict_body

    # Picking Device Id from header
    device_id = dict_body.get("header").get('device_id')

    for info in dict_body.get("recording_info"):

        print(info.get("recording_state"))
        if info.get('events'):
            for event in info.get('events'):
                if event.get("value", "") == '1':
                    # Create a random uuid to identify a given camera health check process
                    uid = str(uuid.uuid4())
                    #payload = {'device_id': device_id}
                    #payload = {}
                    timestamps = event.get('timestamp_ms')
                    cal_date = datetime.fromtimestamp(int(timestamps[:10]))
                    # print(cal_date, timestamps)

                    prev_timestamps = int(datetime.timestamp(cal_date - timedelta(seconds=5)))
                    post_timestamps = int(datetime.timestamp(cal_date + timedelta(seconds=5)))

                    #payload.update({'uid': uid, 'start_time': str(prev_timestamps), 'end_time': str(post_timestamps)})
                    payload = {'from': str(prev_timestamps), 'to': str(post_timestamps)}

                    # Send API request (POST)
                    addr = f"https://dev.bosch-ridecare.com/footage/devices/{device_id}/videofootage"
                    
                    try:
                        headers = {}
                        headers['Content-Type'] = 'application/json'
                        headers['Authorization'] = 'Bearer ' + refresh_api_token().get('access_token')
                        requests.post(addr, data=payload, headers=headers)
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
    #pending_queue = {}

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing request
            request_process_selector(s3_client,
                                         container_services,
                                         message['Body'])

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

        

if __name__ == '__main__':
    main()
