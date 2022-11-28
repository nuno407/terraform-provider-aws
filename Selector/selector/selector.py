""" Selector component bussiness logic. """
from datetime import datetime, timedelta
import logging
from selector.footage_api_wrapper import FootageApiWrapper
_logger = logging.getLogger(__name__)


class Selector():
    """ Class responsible by containing all bussiness logic used in in the Selector component. """

    def __init__(self, sqs_client, container_services, footage_api_wrapper: FootageApiWrapper, hq_queue_name: str):
        self.__sqs_client = sqs_client
        self.__container_services = container_services

        # Define additional input SQS queues to listen to
        # (container_services.input_queue is the default queue
        # and doesn"t need to be declared here)
        self.__hq_queue = hq_queue_name

        self.footage_api_wrapper = footage_api_wrapper

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

    def __process_selector_message(self, message: str):
        message_body = self.__container_services.get_message_body(message)
        _logger.info("Processing selector pipeline message..\n%s", message_body)

        # Picking Device Id from header
        if "value" in message_body and \
            "properties" in message_body["value"] and \
            "recording_info" in message_body["value"]["properties"] and \
                "header" in message_body["value"]["properties"]:
            msg_header = message_body["value"]["properties"]["header"]
            device_id = msg_header.get("device_id")

            recording_info = message_body["value"]["properties"].get("recording_info")

            for event in [event for info in recording_info for event in info.get("events", [])]:
                if event.get("value", "") != "0":
                    timestamps = str(event.get("timestamp_ms"))
                    cal_date = datetime.fromtimestamp(int(timestamps[:10]))

                    prev_timestamp = int(datetime.timestamp(cal_date - timedelta(seconds=5)))
                    post_timestamp = int(datetime.timestamp(cal_date + timedelta(seconds=5)))

                    self.footage_api_wrapper.request_footage(device_id, prev_timestamp, post_timestamp)
        else:
            _logger.info("Not a valid Message")

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
            self.__container_services.delete_message(self.__sqs_client, message["ReceiptHandle"], self.__hq_queue)

    def __process_hq_message(self, message: str):
        message_body = self.__container_services.get_message_body(message)
        _logger.info("Processing HQ pipeline message..\n%s", message_body)

        # Picking Device Id from header
        if all(prop in message_body for prop in ("deviceId", "footageFrom", "footageTo")):
            device_id = message_body["deviceId"]
            from_timestamp = message_body["footageFrom"]
            to_timestamp = message_body["footageTo"]

            self.footage_api_wrapper.request_footage(device_id, from_timestamp, to_timestamp)

        else:
            _logger.info("Not a valid Message")

    def __log_message(self, message, queue="selector"):
        _logger.info("Message contents from %s:\n", queue)
        _logger.info(message)
