from datetime import datetime, timedelta
import json
import logging

import urllib3
from urllib3 import Retry
from api_token_manager import ApiTokenManager

class Selector():
    def __init__(self, sqs_client, container_services):
        self.__sqs_client = sqs_client
        self.__container_services = container_services
        retries = Retry(connect=3, read=1, backoff_factor=1)
        self.__http_client = urllib3.PoolManager(retries=retries)

        # Define additional input SQS queues to listen to
        # (container_services.input_queue is the default queue
        # and doesn't need to be declared here)
        self.__hq_queue = container_services.sqs_queues_list['HQ_Selector']


        # Create Api Token Manager
        self.__api_token_manager = ApiTokenManager(token_endpoint = container_services.api_endpoints['selector_token_endpoint'],
                secret_id = container_services.secret_managers['selector'])

    def handle_selector_queue(self):
        # Check input SQS queue for new messages
        message = self.__container_services.listen_to_input_queue(self.__sqs_client)

        if message:
            # Processing request
            self.__process_selector_message(message)

            # Delete message after processing
            self.__container_services.delete_message(self.__sqs_client,
                                                message['ReceiptHandle'])

    def __process_selector_message(self, message):
        message_body = self.__container_services.get_message_body(message)
        logging.info(f"Processing selector pipeline message..\n{message_body}")

        # Picking Device Id from header
        if "value" in message_body:
            msg_header = message_body["value"]["properties"]["header"]
            device_id = msg_header.get('device_id')
            if "recording_info" in message_body["value"]["properties"]:
                    
                recording_info = message_body["value"]["properties"].get("recording_info")
                for info in recording_info:

                    if info.get('events'):
                        for event in info.get('events'):
                            if event.get("value", "") != '0':
                                timestamps = str(event.get('timestamp_ms'))
                                cal_date = datetime.fromtimestamp(int(timestamps[:10]))

                                prev_timestamp = int(datetime.timestamp(cal_date - timedelta(seconds=5)))
                                post_timestamp = int(datetime.timestamp(cal_date + timedelta(seconds=5)))

                                self.request_footage_api(device_id, prev_timestamp, post_timestamp)

        else:
            logging.info("Not a valid Message")   

    def request_footage_api(self, device_id, from_timestamp, to_timestamp):
        auth_token = self.__api_token_manager.get_token()
        if not auth_token:
            logging.error('Could not get auth token for Footage API. Skipping request.')
            return
        
        payload = {'from': str(from_timestamp), 'to': str(to_timestamp), 'recorder': 'TRAINING'}
        url = self.__container_services.api_endpoints["mdl_footage_endpoint"].format(device_id)

        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + auth_token
        body = json.dumps(payload)

        try:
            logging.info(f'Requesting footage upload from url "{url}" with timestamp from {from_timestamp} to {to_timestamp}')
            response = self.__http_client.request('POST', url, headers=headers, body=body)

            if(response.status >= 200 and response.status <300):
                logging.info(f'Successfully requested footage with response code {response.status}')
            else:
                logging.warning(f'Unexpected response when requesting footage: {response}')
                if response.content:
                    logging.warning(f'Details: {response.content}')
        except Exception as error:
            logging.error(f'Unexpected error occured when requesting footage: {error}')

    def handle_hq_queue(self):
        # Check input SQS queue for new messages
        message = self.__container_services.listen_to_input_queue(self.__sqs_client, self.__hq_queue)

        if message:
            # Processing request
            self.__process_hq_message(message)

            # Delete message after processing
            self.__container_services.delete_message(self.__sqs_client,
                                                message['ReceiptHandle'], self.__hq_queue)

    def __process_hq_message(self, message):
        message_body = self.__container_services.get_message_body(message)
        logging.info(f"Processing HQ pipeline message..\n{message_body}")

        # Picking Device Id from header
        if all(property in message_body for property in('deviceId', 'footageFrom', 'footageTo')):
            device_id = message_body['deviceId']
            from_timestamp = str(message_body['footageFrom'])
            to_timestamp = str(message_body['footageTo'])

            self.request_footage_api(device_id, from_timestamp, to_timestamp)

        else:
            logging.info("Not a valid Message")   
