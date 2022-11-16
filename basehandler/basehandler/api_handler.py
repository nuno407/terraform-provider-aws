""" Entry Module that wraps IVS Chain models. """
import logging
import queue
from threading import Thread

import flask
from flask import Blueprint, Flask

from base.aws.container_services import ContainerServices
from base.aws.shared_functions import AWSServiceClients
from basehandler.message_handler import ErrorMessage, InternalMessage


class OutputEndpointParameters():  # pylint: disable=too-few-public-methods
    """ OutputEndpointParameters """

    def __init__(self, container_services: ContainerServices,
                 mode: str,
                 aws_clients: AWSServiceClients,
                 internal_queue: queue.Queue[InternalMessage] = queue.Queue(
                     maxsize=1),
                 ) -> None:
        """
        This is a wrapper for APIHandler endpoint arguments

        Args:
            container_services (ContainerServices): instance of the ContainerServices object
            mode (str): IVS feature chain processing mode.
            aws_clients (AWSServiceClients): wrapper for boto3 AWS clients
            internal_queue (queue.Queue): internal queue
        """

        self.container_services = container_services
        self.aws_clients = aws_clients
        self.internal_queue = internal_queue
        self.mode = mode


class OutputEndpointNotifier():
    """
    OutputEndpointParameters
    It is used by the Output endpoint to send information to the message handler.
    """

    def __init__(self, endpoint_params: OutputEndpointParameters):
        self.internal_queue = endpoint_params.internal_queue
        self.container_services = endpoint_params.container_services
        self.aws_clients = endpoint_params.aws_clients

    def notify_error(self) -> None:
        """
        Notifies the message handler that an error as ocurred on processing of the current message
        """
        self.internal_queue.put(ErrorMessage())

    def upload_and_notify(self, chunk: list, path: str, internal_message: InternalMessage) -> None:
        """
        Uploads file to specified path, sends update message to respective handler

        Args:
            chunk (list): Binary data to be uploaded
            path (str): The S3 path to be uploaded to
            internal_message (InternalMessage): The message to be passed to the MessageHandler

        Raises:
            TypeError: If internal_message is not parsed from JSON format
        """

        # Make sure that the message is parsed
        if not isinstance(internal_message, InternalMessage):
            raise TypeError("Message body needs to be parsed from JSON")

        # Upload file to S3 bucket
        self.container_services.upload_file(
            self.aws_clients.s3_client, chunk, self.container_services.anonymized_s3, path)

        # Set status to OK
        internal_message.status = InternalMessage.Status.OK

        # Send message to input queue of metadata container
        self.internal_queue.put(internal_message)


class NoHealth(logging.Filter):  # pylint: disable=too-few-public-methods
    """ Creates a filter to prevent logging of GetAlive endpoint. """

    def filter(self, record):
        return "GET /alive" not in record.getMessage()


class APIHandler():  # pylint: disable=too-few-public-methods
    """ APIHandler """

    def __init__(self,
                 endpoint_params: OutputEndpointParameters,
                 callback_blueprint: Blueprint,
                 endpoint_notifier: OutputEndpointNotifier) -> None:
        """
        Creates an APIHandler that will receive the callback from a processing request to the IVS Feature chain.
        It should receive a blueprint that post a message to the internal queue after receiving the callback request.
        This should trigger the MessageHandler output post-processing flow.

        Args:
            endpoint_params (OutputEndpointParameters):
            callback_blueprint (Blueprint): flask blueprint to be registered by the app when creating the routes.
            message_handler_thread (Thread): reference to the message_handler_thread to be available in the
                                             alive healthcheck endpoint
            endpoint_notifier (OutputEndpointNotifier): To notify in case of an error ocurred
        """
        self.container_services = endpoint_params.container_services
        self.callback_blueprint = callback_blueprint
        self.mode = endpoint_params.mode
        self.endpoint_notifier = endpoint_notifier

        app = Flask(__name__)
        self.app = app

    def create_routes(self) -> flask.Flask:
        """
        Create flask API routes and register provided blueprint

        Returns:
            app (Flask): flask app
        """
        app = self.app
        app.register_blueprint(self.callback_blueprint)
        logging.getLogger("werkzeug").addFilter(NoHealth())

        @app.route("/alive", methods=["GET"])
        def handle_alive():
            return flask.Response(status=200, response="Ok")

        @app.route("/processingerror", methods=["POST"])
        def handle_processing_error():
            self.endpoint_notifier.notify_error()
            return flask.Response(status=200, response="Ok")

        return app
