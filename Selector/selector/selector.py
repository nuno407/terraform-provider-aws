""" Selector component bussiness logic. """
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

    def handle_hq_queue(self):
        """ Function responsible for processing messages from SQS (component entrypoint). """
        # Check input SQS queue for new messages
        message = self.__container_services.listen_to_input_queue(self.__sqs_client, self.__hq_queue)

        if message:
            # save some messages as examples for development
            self.log_message(message, "selector HQ")
            # Processing request
            success = self.__process_hq_message(message)
            if success:
                # Delete message after processing
                self.__container_services.delete_message(self.__sqs_client, message["ReceiptHandle"], self.__hq_queue)

    def __process_hq_message(self, message: str) -> bool:
        """Logic to call the footage upload request

        Args:
            message (str): message read from the queue

        Returns:
            bool: Boolean indicating if the request succeeded.
        """
        message_body = self.__container_services.get_message_body(message)
        _logger.info("Processing HQ pipeline message..\n%s", message_body)

        # Picking Device Id from header
        if all(prop in message_body for prop in ("deviceId", "footageFrom", "footageTo")):
            device_id = message_body["deviceId"]
            from_timestamp = message_body["footageFrom"]
            to_timestamp = message_body["footageTo"]

            try:
                self.footage_api_wrapper.request_footage(device_id, from_timestamp, to_timestamp)
                return True
            except Exception as error:  # pylint: disable=broad-except
                _logger.error("Unexpected error occured when requesting footage: %s", error)
                return False
        else:
            _logger.info("Not a valid Message")
            return False

    def log_message(self, message, queue="selector"):
        """Logs messages

        Args:
            message (str): Message to be logged
            queue (str, optional): Queue where the message came from. Defaults to "selector".
        """
        _logger.info("Message contents from %s:\n", queue)
        _logger.info(message)
