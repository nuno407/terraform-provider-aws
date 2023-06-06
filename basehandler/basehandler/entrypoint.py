""" Entrypoint. """
import os
from multiprocessing import Process, Queue
from typing import Protocol
import logging

from flask import Blueprint

from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.aws.shared_functions import AWSServiceClients
from basehandler.api_handler import (APIHandler, OutputEndpointNotifier,
                                     OutputEndpointParameters)
from basehandler.message_handler import (InternalMessage, MessageHandler,
                                         NOOPPostProcessor, PostProcessor)

INTERNAL_QUEUE_MAX_SIZE = int(os.getenv("INTERNAL_QUEUE_MAX_SIZE", "1"))

_logger: logging.Logger = ContainerServices.configure_logging("basehandler")


class CallbackBlueprintCreator(Protocol):  # pylint: disable=too-few-public-methods
    """
    Interface for importing services to provide their own bluprint creator functions
    """
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        """
        Function responsible for creating the BluePrint for the endpoint

        Args:
            route_endpoint (str): Route endpoint e.g : "/processingSuccess"
            notifier (OutputEndpointNotifier): And instance of OutputEndpointNotifier to
                                               upload and send data to the MessageHandler

        Raises:
            NotImplementedError: In case the didn"t get implemented by the derived class

        Returns:
            Blueprint: A blueprint of an endpoint to be added to the API
        """
        # protocol function defines the signature and has and empty body
        raise NotImplementedError()


class BaseHandler():
    """ BaseHandler """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 container_name: str,
                 container_services: ContainerServices,
                 aws_clients: AWSServiceClients,
                 mode: str,
                 route_endpoint: str,
                 blueprint_creator: CallbackBlueprintCreator) -> None:
        """
        Creates a BaseHandler responsible for loading and validating the container services configurations
        also creates an internal queue for communication between the MessageHandler and APIHandler,
        the post processing upload notifier (OutputEndpointNotifier) and the API callback blueprint

        Args:
            container_name (str): current microservice contianer name.
            container_service (ContainerServices): instance of ContainerServices object.
            aws_client (AWSServiceClients): boto3 aws service clients wrapper object.
            mode (str): IVS feature chain processing mode.
            route_endpoint (str): endpoint which APIHandler will wait for IVS feature chain response.
            blueprint_creator (CallbackBlueprintCreator): function reference that will provide a
                                                          blueprint for the callback API endpoint
        """

        self.container_name = container_name
        self.container_services = container_services
        self.aws_clients = aws_clients
        self.mode = mode

        self.container_services.load_config_vars()
        self.validate_container_services_config()

        internal_queue: Queue[InternalMessage] = Queue(
            maxsize=int(INTERNAL_QUEUE_MAX_SIZE))
        self.endpoint_params = OutputEndpointParameters(
            self.container_services,
            mode,
            aws_clients,
            internal_queue)

        self.endpoint_notifier = OutputEndpointNotifier(self.endpoint_params)
        self.callback_blueprint = blueprint_creator.create(
            route_endpoint, self.endpoint_notifier)

    def validate_container_services_config(self):
        """
        Checks if the loaded configurations in ContainerServices is valid
        should be called after ContainerServices.load_config_vars()

        Raises:
            RuntimeError: if the configuration is invalid
        """
        if len(self.container_services.sqs_queues_list) == 0 \
                or not self.container_services.raw_s3 \
                or not self.container_services.anonymized_s3:
            raise RuntimeError("invalid container services configuration")

    def setup_and_run(self,
                      api_port: str,
                      graceful_exit: GracefulExit,
                      post_processor: PostProcessor = NOOPPostProcessor()) -> None:
        """
        Starts the APIHandler and MessageHandler threads

        Args:
            api_port (str): port that the flask API will bind to
            graceful_exit (GracefulExit): Exit handler to determine if processing should end
            post_processor (PostProcessor): object that implements PostProcessor interface
                                            to be executed after ivs feature chain
        """

        # MessageHandler will be executed in a new thread
        message_handler = MessageHandler(
            self.endpoint_params.container_services,
            self.endpoint_params.aws_clients,
            self.container_name,
            self.endpoint_params.internal_queue,
            post_processor
        )

        import basehandler.message_handler as msg_handler  # pylint: disable=import-outside-toplevel

        api_handler = APIHandler(self.endpoint_params,
                                 self.callback_blueprint,
                                 self.endpoint_notifier)
        api = api_handler.create_routes()

        api_process = Process(target=api.run,
                              kwargs={"host": "0.0.0.0", "port": int(api_port)})  # nosec this is as intended
        _logger.info("Starting api child process")
        api_process.start()

        if msg_handler.wait_for_featurechain() is not None:
            message_handler.start(self.mode, graceful_exit)
        else:
            _logger.exception("Could not connect to IVS container, will shut down.")

        _logger.info("Going to terminate API")
        # Relying on terminate() is error-prone and needs a proper signal handler on the process.
        # In our case the API doesn't do any long-running tasks worth waiting for, therefore
        # just killing the process directly.
        api_process.kill()
        _logger.info("Just terminated the API")
