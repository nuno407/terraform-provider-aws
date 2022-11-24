""" Selector component bussiness logic. """
from datetime import datetime, timedelta
import json
import logging

import urllib3
from urllib3 import Retry
from selector.api_token_manager import ApiTokenManager

_logger = logging.getLogger(__name__)


class Selector():
    """ Class responsible by containing all bussiness logic used in in the Selector component. """

    def __init__(self, sqs_client, container_services):
        self.__sqs_client = sqs_client
        self.__container_services = container_services
        retries = Retry(total=3, backoff_factor=1, allowed_methods=["POST"], status_forcelist=[500])
        self.__http_client = urllib3.PoolManager(retries=retries)

        # Define additional input SQS queues to listen to
        # (container_services.input_queue is the default queue
        # and doesn"t need to be declared here)
        self.__hq_queue = container_services.sqs_queues_list["HQ_Selector"]

        # Create Api Token Manager
        self.__api_token_manager = ApiTokenManager(
            token_endpoint=container_services.api_endpoints["selector_token_endpoint"],
            secret_id=container_services.secret_managers["selector"])

    def handle_selector_queue(self):
        """ Function responsible for processing messages from SQS (component entrypoint). """
        # Check input SQS queue for new messages
        message = self.__container_services.listen_to_input_queue(self.__sqs_client)

        if message:
            self.__log_message(message)
            # Processing request
            self.__process_selector_message(message)

            # Delete message after processing
            self.__container_services.delete_message(self.__sqs_client, message["ReceiptHandle"])

    def __process_selector_message(self, message):
        message_body = self.__container_services.get_message_body(message)
        _logger.info("Processing selector pipeline message..\n%s", message_body)

        # Picking Device Id from header
        if "value" in message_body and "recording_info" in message_body["value"]["properties"]:
            msg_header = message_body["value"]["properties"]["header"]
            device_id = msg_header.get("device_id")

            recording_info = message_body["value"]["properties"].get("recording_info")

            for event in [event for info in recording_info for events in info.get("events", []) for event in events]:
                if event.get("value", "") != "0":
                    timestamps = str(event.get("timestamp_ms"))
                    cal_date = datetime.fromtimestamp(int(timestamps[:10]))

                    prev_timestamp = int(datetime.timestamp(cal_date - timedelta(seconds=5)))
                    post_timestamp = int(datetime.timestamp(cal_date + timedelta(seconds=5)))

                    self.__request_footage_api(device_id, prev_timestamp, post_timestamp)
        else:
            _logger.info("Not a valid Message")

    def __request_footage_api(self, device_id, from_timestamp, to_timestamp):
        auth_token = self.__api_token_manager.get_token()
        if not auth_token:
            _logger.error("Could not get auth token for Footage API. Skipping request.")
            return

        payload = {"from": str(from_timestamp), "to": str(to_timestamp), "recorder": "TRAINING"}
        url = self.__container_services.api_endpoints["mdl_footage_endpoint"].format(device_id)

        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = "Bearer " + auth_token
        body = json.dumps(payload)

        try:
            _logger.info(
                "Requesting footage upload from url '%s' with timestamp from %i to %i",
                url,
                from_timestamp,
                to_timestamp)
            response = self.__http_client.request("POST", url, headers=headers, body=body)

            if (response.status >= 200 and response.status < 300):
                _logger.info("Successfully requested footage with response code %i", response.status)
            else:
                _logger.warning("Unexpected response when requesting footage: %i*", response.status)
                if response.content:
                    _logger.warning("Details: %s", response.content)
        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting footage: %s", error)

    def handle_hq_queue(self):
        """ Function responsible for processing messages from SQS (component entrypoint). """
        # Check input SQS queue for new messages
        message = self.__container_services.listen_to_input_queue(self.__sqs_client, self.__hq_queue)

        if message:
            # save some messages as examples for development
            self.__log_message(message, "selector HQ")
            # Processing request
            self.__process_hq_message(message)

            # Delete message after processing
            self.__container_services.delete_message(self.__sqs_client,
                                                     message["ReceiptHandle"], self.__hq_queue)

    def __process_hq_message(self, message):
        message_body = self.__container_services.get_message_body(message)
        _logger.info("Processing HQ pipeline message..\n%s", message_body)

        # Picking Device Id from header
        if all(property in message_body for property in ("deviceId", "footageFrom", "footageTo")):
            device_id = message_body["deviceId"]
            from_timestamp = str(message_body["footageFrom"])
            to_timestamp = str(message_body["footageTo"])

            self.__request_footage_api(device_id, from_timestamp, to_timestamp)

        else:
            _logger.info("Not a valid Message")

    def __log_message(self, message, queue="selector"):
        _logger.info("Message contents from %s:\n", queue)
        _logger.info(message)
