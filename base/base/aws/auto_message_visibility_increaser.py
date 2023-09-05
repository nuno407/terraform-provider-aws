""" Module for auto MessageVisibility Increaser. """
import threading
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
        while not self._stop_event.wait(timeout=self.interval):
            _logger.info("Increasing visibility timeout.")
            self.container_services.update_message_visibility(
                self.sqs_client, self.receipt_handle, self.interval + 30, input_queue=self.input_queue)

    def stop(self):
        """Stop the _increase_visibility_timeout thread gracefully."""
        self._stop_event.set()

    def __init__(self, sqs_client,  # pylint: disable=too-many-arguments
                 receipt_handle: str,
                 container_services: ContainerServices,
                 interval: float,
                 input_queue: str):
        self._stop_event = threading.Event()
        self.sqs_client = sqs_client
        self.receipt_handle = receipt_handle
        self.container_services = container_services
        self.interval = interval
        self.input_queue = input_queue
        self.visibility_process = threading.Thread(
            target=AutoMessageVisibilityIncreaser._increase_visibility_timeout, args=[self])

    def __enter__(self):
        self.visibility_process.start()

    def __exit__(self, _type, _value, _traceback):
        self.stop()
        self.visibility_process.join()
        _logger.debug("AutoMessageVisibilityIncreaser process shut down")
