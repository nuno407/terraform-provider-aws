""" Selector component bussiness logic. """
import logging

from kink import inject
from mypy_boto3_sqs.type_defs import MessageTypeDef

from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from base.model.artifacts import parse_artifact
from selector.footage_api_wrapper import FootageApiWrapper

_logger = logging.getLogger(__name__)


@inject
class Selector:
    """ Class responsible by containing all bussiness logic used in in the Selector component. """

    def __init__(self,
                 footage_api_wrapper: FootageApiWrapper,
                 sqs_controller: SQSController):
        self.__sqs_controller = sqs_controller
        self.footage_api_wrapper = footage_api_wrapper

    @inject
    def run(self, graceful_exit: GracefulExit) -> None:
        """ Function responsible for running the component (component entrypoint). """
        _logger.info("Starting Selector..")

        while graceful_exit.continue_running:
            message = self.__sqs_controller.get_message()
            if not message:
                continue
            self.handle_hq_queue(message)

    def handle_hq_queue(self, message: MessageTypeDef):
        """ Function responsible for processing messages from SQS (component entrypoint). """
        _logger.info("handling message -> %s", message)
        success = self.__process_hq_message(message)
        if success:
            self.__sqs_controller.delete_message(message)

    def __process_hq_message(self, message: MessageTypeDef) -> bool:
        """Logic to call the footage upload request

        Args:
            message (str): message read from the queue

        Returns:
            bool: Boolean indicating if the request succeeded.
        """
        message_body = message["Body"]
        _logger.info("Processing HQ pipeline message..\n%s", message_body)

        video_artifact = parse_artifact(message_body)

        device_id = video_artifact.device_id
        from_timestamp = int(video_artifact.timestamp.timestamp() * 1000)
        to_timestamp = int(video_artifact.end_timestamp.timestamp() * 1000)
        try:
            self.footage_api_wrapper.request_recorder("TRAINING",
                                                      device_id,
                                                      from_timestamp,
                                                      to_timestamp)
            self.footage_api_wrapper.request_recorder("TRAINING_MULTI_SNAPSHOT",
                                                      device_id,
                                                      from_timestamp,
                                                      to_timestamp)
            return True
        except Exception as error:  # pylint: disable=broad-except
            _logger.error("Unexpected error occured when requesting footage: %s", error)
            return False
