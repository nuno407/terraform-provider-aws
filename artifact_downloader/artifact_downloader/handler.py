""" downloader handler module """

import logging

from kink import inject

from requests.exceptions import ConnectionError as RequestsConnectionError
from base.aws.sqs import SQSController
from base.graceful_exit import GracefulExit
from artifact_downloader.http_client import HttpClient
from artifact_downloader.message.message_handler import MessageHandler
from artifact_downloader.exceptions import UnexpectedReturnCode

_logger = logging.getLogger(__name__)


@inject
class Handler():  # pylint: disable=too-few-public-methods
    """ Central logic handler for downloader component """

    def __init__(
            self,
            message_handler: MessageHandler,
            sqs_controller: SQSController,
            graceful_exit: GracefulExit,
            http_client: HttpClient) -> None:
        self.__message_handler = message_handler
        self.__sqs_controller = sqs_controller
        self.__graceful_exit = graceful_exit
        self.__http_client = http_client

    def run(self):
        """ main entry point """
        while self.__graceful_exit.continue_running:
            message = self.__sqs_controller.get_message()

            if not message:
                _logger.info("Waiting for a new message")
                continue

            try:
                request = self.__message_handler.handle(message)
                self.__http_client.execute_request(request)
                self.__sqs_controller.delete_message(message)
            except UnexpectedReturnCode as excpt:
                _logger.error("Error executing the API request (%s)", str(excpt))

            except RequestsConnectionError as excpt:
                _logger.error("Error reaching endpoint. (%s)", str(excpt))
