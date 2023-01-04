""" Implementation of logic associated with ivs chain queue processing. """
import json
import logging
import os
import queue
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from time import perf_counter
from typing import Optional, Protocol

import backoff
import requests
from requests.status_codes import codes as status_codes

from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import AWSServiceClients
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS

_logger: logging.Logger = ContainerServices.configure_logging('basehandler')

# defaults to 10h
INTERNAL_QUEUE_TIMEOUT = float(os.getenv("INTERNAL_QUEUE_TIMEOUT", "36000"))
IVS_FC_HOSTNAME = os.getenv("IVS_FC_HOSTNAME", "localhost")
IVS_FC_PORT = os.getenv("IVS_FC_PORT", "8081")
IVS_FC_STATUS_ENDPOINT = f"http://{IVS_FC_HOSTNAME}:{IVS_FC_PORT}/status"
IVS_FC_MAX_WAIT = float(os.getenv("IVS_FC_MAX_WAIT", "120"))


@dataclass
class InternalMessage:
    """Message baseclass for internal communication between the APIHandler and MessageHandler"""
    class Status(Enum):
        """InternalMessage status enum"""
        PROCESSING_COMPLETED = auto()
        OK = auto()
        ERROR = auto()
    status: Status


@dataclass
class OperationalMessage(InternalMessage):
    """Specialisation of InternalMessage in case of no errors"""
    uid: str
    bucket: str
    input_media: str
    media_path: Optional[str] = None
    meta_path: Optional[str] = None


@dataclass
class ErrorMessage(InternalMessage):
    """Specialisation of InternalMessage that indicates errors"""
    status: InternalMessage.Status = InternalMessage.Status.ERROR


class PostProcessor(Protocol):  # pylint: disable=too-few-public-methods
    """Interface to be implemented by any service that uses the MessageHandler.

    The service shall pass an instance that implements this type to the MessageHandler in order to be able
    to do some postprocessing.
    """
    @abstractmethod
    def run(self, message_body: OperationalMessage) -> None:
        """Function the will be run after the output from the processing algorithm is done.

        It needs to be implemented by the derived class.

        Args:
            message_body (OperationalMessage): The message sent from processing algorithm that can be changed.
        """
        raise NotImplementedError()  # protocol function defines the signature and has and empty body


class NOOPPostProcessor(PostProcessor):  # pylint: disable=too-few-public-methods
    """Implementation of PostProcessing that does no pre-processing.

    An instance of this type shall be passed to the MessageHandler if post processing is not needed.

    """

    def run(self, message_body: OperationalMessage) -> None:
        """NOOP function that doesn"t do any post processing.

        Args:
            message_body (OperationalMessage): The message sent from processing algorithm that can be changed.
        """
        print(f"message {message_body}")
        print("No post-processing required")


class FileIsEmptyException(Exception):
    """File is empty should be raise when a snapshot or video has 0 bytes."""

    def __init__(self, message):
        super().__init__(message)


class ProcessingOutOfSyncException(Exception):
    """ Processing out of sync should be raised when the processing is out of sync, i.e.
        the processed item is not the expected one. """

    def __init__(self, message):
        super().__init__(message)


class EmptyAPIOutputMessage(Exception):
    """Empty API output message should be raise when an internal queue message is None."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RequestProcessingFailed(Exception):
    """Requesting processing failed exception raised when processing request fails.

    Should be treated by restarting the container pod
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class MessageHandler():
    """ Message handler """

    # pylint: disable=too-many-arguments
    def __init__(
            self,
            container_services: ContainerServices,
            aws_clients: AWSServiceClients,
            consumer_name: str,
            internal_queue: queue.Queue[InternalMessage],
            post_processor: PostProcessor) -> None:
        """
        Creates a MessageHandler service that will handle the messages between the algorithm processing service.
        The `consumer_name` will be the name used to grab messages from the queue.

        The Handler keeps listening for a message from it"s sqs queue,
        once a message is received is then sent to the algorithm processing service,
        it will then wait for a message in the internal queue.
        Once the algorithm processing service has finished it will send a request to the APIHandler which will put
        the message in the internal queue where the MessageHandler will grab it and run a post processing method.
        The next message on the SQS queue is only ingested after an answer is received from the internal
        queue or the timeout is reached (INTERNAL_QUEUE_TIMEOUT).

        Remarks:
        The internal queue is just used as synchronization mechanism to avoid race conditions,
        it"s size can"t grow bigger then 1.

        The message pass on the internal queue has to have the following format:
                {
                "chunk": None,
                "path": None,
                "msg_body": None,
                "status" : "ERROR"
                }

        Args:
            container_services (ContainerServices): An instance of ContainerServices
            aws_clients (AWSServiceClients): An instance of AWSServicesClients containing the required clients
            consumer_name (str): The name of the consumer service
            internal_queue (queue.Queue): A queue that used as a synchronization mechanism between the API and
                                          the handler
            post_processor (PostProcessor): A class that implements PostProcessor that will be run after
                                            the return from the algorithm processing service.
        """
        self.__container_services = container_services
        self.__aws_clients = aws_clients
        self.__consumer_name = consumer_name
        self.__internal_queue = internal_queue
        self.__post_processor = post_processor

    def parse_incoming_message_body(self, body: str) -> dict:
        """
        transform incoming message body from dict __repr___ to valid JSON and parse it into a dict

        Args:
            body (str): body as dict in string __repr__
        Returns:
            dict: parsed body
        """
        new_body = body.replace("'", "\"")
        return json.loads(new_body)

    def request_processing(self, body: str, mode: str) -> bool:  # pylint: disable=too-many-locals
        """
        Responsible for making the POST request to the algorithm processing service.
        This return code of this function does not mean that the algorithm processing
        service completed it"s processing with success, it only gives the status whether it was successful or not.

        Args:
            body (str): The message body in JSON got from the SQS queue, before parsing.
            mode (str): IVS feature chain procesing mode.

        Returns:
            bool: Returns True if the requests was successfully or false otherwise
        """
        _logger.info("Processing pipeline message..")
        client = self.__aws_clients.s3_client
        container_services = self.__container_services

        _logger.info("Raw message body: %s", body)

        dict_body: dict = self.parse_incoming_message_body(body)

        raw_file = container_services.download_file(
            client, container_services.raw_s3, dict_body["s3_path"])
        if not raw_file:
            raise FileIsEmptyException(
                f"File {dict_body['s3_path']} is empty")

        uid = str(uuid.uuid4())

        payload = {"uid": uid, "path": dict_body["s3_path"], "mode": mode}
        file_ext = os.path.splitext(dict_body["s3_path"])[1].replace(".", "")

        # Check if the extension is a known image format or video format
        if file_ext in VIDEO_FORMATS:
            file_format = "video"
        elif file_ext in IMAGE_FORMATS:
            file_format = "image"
        else:
            # Throw an error if the extension is not known
            _logger.exception(
                "The snapshot/video %s has an unknown format %s", dict_body["s3_path"], file_ext)
            return False

        files = [(file_format, raw_file)]

        port_pod = container_services.ivs_api["port"]
        req_command = container_services.ivs_api["endpoint"]

        address = f"http://{IVS_FC_HOSTNAME}:{port_pod}/{req_command}"
        response = requests.post(address, files=files,
                                 data=payload, timeout=IVS_FC_MAX_WAIT)
        _logger.info("API POST request sent! (uid: %s)", payload["uid"])
        _logger.info("Response: %s", response.text)
        return response.status_code == status_codes.ok  # pylint: disable=no-member

    def update_processing(self, incoming_message_body: dict) -> dict:
        """
        Updates the processing steps by removing the current service from it (if needed) and updating the data_status.
        Note: The message passed as argument will be changed.

        Args:
            incoming_message_body (dict): Parsed JSON message grabbed from the SQS queue.

        Returns:
            dict: The changed message. (The same as the input)
        """
        consumer_name = self.__consumer_name

        # Remove current step/container from the processing_steps
        # list (after processing)
        if incoming_message_body["processing_steps"][0] == consumer_name:
            incoming_message_body["processing_steps"].pop(0)

        if incoming_message_body["processing_steps"]:
            # change the current file data_status (if not already changed)
            incoming_message_body["data_status"] = "processing"
        else:
            # change the current file data_status to complete
            # (if current step is the last one from the list)
            incoming_message_body["data_status"] = "complete"

        return incoming_message_body

    def handle_processing_output(self, incoming_message: dict, parsed_output_message_body: OperationalMessage) -> None:
        """
        Function responsible for handling the messages from the algorithm processing service.

        - Runs the post processing function specified by the parent service.
        - Updates processing steps and current processing state.
        - Sends the processing state to the Metadata queue.

        Args:
            incoming_message (dict): JSON message grabbed from the SQS queue.
            parsed_output_message_body (OperationalMessage): Parsed JSON message grabbed from the internal queue.
        """

        post_processor = self.__post_processor
        container_services = self.__container_services
        aws_clients = self.__aws_clients

        parsed_incoming_message_body = self.parse_incoming_message_body(
            incoming_message["Body"])

        # Validate if the processed recording is the same as the one that was sent to the IVS feature chain
        if parsed_incoming_message_body["s3_path"] != parsed_output_message_body.input_media:
            raise ProcessingOutOfSyncException(
                f"""Processed recording {parsed_output_message_body.input_media} is different than the one that
                    was sent to the IVS feature chain {parsed_incoming_message_body["s3_path"]}""")

        post_processor.run(parsed_output_message_body)

        relay_list = self.update_processing(parsed_incoming_message_body)

        # Retrieve output info from received message
        out_s3 = {}
        out_s3["bucket"] = parsed_output_message_body.bucket
        # If no media_path is provided, use "-"
        out_s3["media_path"] = parsed_output_message_body.media_path or "-"
        # If no meta_path is provided, use "-"
        out_s3["meta_path"] = parsed_output_message_body.meta_path or "-"

        # Send message to input queue of the next processing step
        # (if applicable)
        if relay_list["processing_steps"]:
            next_step = relay_list["processing_steps"][0]
            next_queue = container_services.sqs_queues_list[next_step]
            relay_list_json = json.dumps(relay_list)
            _logger.info("sending message body: %s", relay_list_json)
            container_services.send_message(
                aws_clients.sqs_client, next_queue, relay_list_json)

        # Add the algorithm output flag/info to the relay_list sent
        # to the metadata container so that an item for this processing
        # run can be created on the Algo Output DB
        relay_list["output"] = out_s3
        del relay_list["processing_steps"]

        # Send message to input queue of metadata container
        metadata_queue = container_services.sqs_queues_list["Metadata"]
        relay_list_json = json.dumps(relay_list)
        _logger.info("sending message body: %s", relay_list_json)
        container_services.send_message(
            aws_clients.sqs_client, metadata_queue, relay_list_json)

    def handle_incoming_message(self, incoming_message: dict, mode: str) -> None:
        """
        Function responsible for handling the messages from the input sqs queues.

        Args:
            incoming_message (dict): JSON message grabbed from the SQS queue.
            mode (str): IVS feature chain procesing mode.

        Raises:
            RuntimeError: If the request wasn"t successful.
        """
        request_result = self.request_processing(
            incoming_message["Body"], mode)
        if not request_result:
            raise RuntimeError(
                "Unable to request processing to ivs feature chain")

    def on_process(self, mode: str) -> None:
        """
        Function that will be run in a loop.

        - Wait for a message in the input queue.
        - Handles the message
        - Wait for the message in the internal queue
        - Handles the message from the algorithm processing service
        - Deletes the message from the SQS queue

        Args:
            mode (str): IVS feature chain procesing mode.

        Raises:
            err: Raises any exception caught.
        """
        container_services = self.__container_services
        internal_queue = self.__internal_queue
        aws_clients = self.__aws_clients
        incoming_message = container_services.listen_to_input_queue(
            aws_clients.sqs_client)

        if not incoming_message:
            _logger.warning("Incoming SQS message is None")
            return

        if "Body" not in incoming_message:
            _logger.error("Incoming SQS message has no body")
            return

        sqs_message_body = incoming_message["Body"]

        start_ivs_fc_time = perf_counter()
        parsed_message = self.parse_incoming_message_body(sqs_message_body)
        artifact_key = parsed_message["s3_path"]
        try:
            self.handle_incoming_message(incoming_message, mode)

            # Wait for message to reach internal queue
            api_output_message = internal_queue.get(
                timeout=INTERNAL_QUEUE_TIMEOUT)

            if not api_output_message:
                raise EmptyAPIOutputMessage(
                    "Handler api output message is None")

            stop_ivs_fc_time = perf_counter()

            # If the status message is not OK raises an exception
            if not (isinstance(api_output_message, OperationalMessage)
                    and api_output_message.status == InternalMessage.Status.OK):
                raise RequestProcessingFailed(
                    f"The IVS Chain as failed to process artifact: {artifact_key}")

            inference_duration = stop_ivs_fc_time - start_ivs_fc_time
            _logger.info("IVS process completed: %s time in seconds :: %f",
                         artifact_key, inference_duration)
            self.handle_processing_output(
                incoming_message, api_output_message)
            container_services.delete_message(
                aws_clients.sqs_client, incoming_message["ReceiptHandle"])
        except FileIsEmptyException:
            _logger.warning(
                "Downloaded file %s with zero bytes of length, skipping it", artifact_key)
            container_services.delete_message(
                aws_clients.sqs_client, incoming_message["ReceiptHandle"])
        except EmptyAPIOutputMessage:
            _logger.warning(
                "Internal handler queue output is None, skipping it")
        except RequestProcessingFailed:
            _logger.error(
                "Unable to request processing to ivs feature chain. \
                    Inference container might be in inconsistent state.")
            raise
        except ProcessingOutOfSyncException:
            _logger.error(
                "Processed recording is different than the one that was sent to the IVS feature chain.")
            raise
        except queue.Empty:
            _logger.exception(
                "The timeout of %s seconds while waiting for the IVS Chain has been reached. \
                    The message was not deleted from the SQS Queue", INTERNAL_QUEUE_TIMEOUT)
            raise
        except Exception:
            _logger.exception("Unexpected exception has happened.")
            raise

    def start(self, mode: str) -> None:
        """
        Setup graceful exit, start main consumer loop function and exit afterwards

        Args:
            mode (str): IVS feature chain procesing mode.
        """
        graceful_exit = GracefulExit()
        _logger.info("Listening to SQS input queue(s)...")
        self.consumer_loop(mode, graceful_exit)
        _logger.info("Exiting after completion of processing.")

    def consumer_loop(self, mode: str, graceful_exit: GracefulExit):
        """consumer_loop.

        Main consumer loop function.

        Args:
            mode (str): _description_
            graceful_exit (GracefulExit): _description_
        """
        while graceful_exit.continue_running:
            try:
                self.on_process(mode)
            except Exception:
                _logger.info("Exiting consumer loop due to exception.")
                break


def on_backoff_handler(details):
    """ Handler for backoff on IVS. """
    _logger.info("Backing off %.1f seconds after %s tries", details.get("wait"), details.get("tries"))


def on_success_handler(details):
    """ Handler for success of ivs API"""
    elapsed = f"{details['elapsed']:0.1f}"
    _logger.info("Got response from IVSFC API: %s after %s seconds",
                 IVS_FC_STATUS_ENDPOINT, elapsed)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_time=IVS_FC_MAX_WAIT,
    raise_on_giveup=True,
    on_backoff=on_backoff_handler,
    on_success=on_success_handler
)
def wait_for_featurechain():
    """ Handler for exception on IVS API. """
    _logger.info("Waiting for IVSFC API on: %s", IVS_FC_STATUS_ENDPOINT)
    requests.get(IVS_FC_STATUS_ENDPOINT)  # pylint: disable=missing-timeout
