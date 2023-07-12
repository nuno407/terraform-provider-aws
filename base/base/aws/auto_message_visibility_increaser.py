""" Module for auto MessageVisibility Increaser. """
import multiprocessing
import time
from base.aws.container_services import ContainerServices

_logger = ContainerServices.configure_logging(__name__)


class AutoMessageVisibilityIncreaser:
    """
        Creates a context that updates the visibility timeout of the given message.
        Please note that the sqs message should not be deleted from the queue from inside the context message!
        Usage:
            with AutoMessageVisibilityIncreaser(sqs_client, Receipt_handle, container_services, 60, input_queue):
                process()
            delete_message() # outside context!

    """

    def _increase_visibility_timeout(self):
        while True:
            time.sleep(self.interval)
            _logger.info("Increasing visibility timeout.")
            self.container_services.update_message_visibility(
                self.sqs_client, self.receipt_handle, self.interval + 30, input_queue=self.input_queue)

    def __init__(self, sqs_client,  # pylint: disable=too-many-arguments
                 receipt_handle: str,
                 container_services: ContainerServices,
                 interval: float,
                 input_queue: str):
        self.sqs_client = sqs_client
        self.receipt_handle = receipt_handle
        self.container_services = container_services
        self.interval = interval
        self.input_queue = input_queue
        self.visibility_process = multiprocessing.Process(
            target=AutoMessageVisibilityIncreaser._increase_visibility_timeout, args=[self])

    def __enter__(self):
        self.visibility_process.start()

    def __exit__(self, _type, _value, _traceback):
        self.visibility_process.kill()
        self.visibility_process.join()
        _logger.info("AutoMessageVisibilityIncreaser process killed")
