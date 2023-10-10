"""MDFParser container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import IMUProcessingResult, SignalsProcessingResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import MDFParserMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class MDFParserContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """MDFParser container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Cosntructor

        Args:
            request_factory (RequestFactory): factory_request
        """
        self.__request_factory = request_factory

        self.__endpoint_imu = PartialEndpoint.RC_IMU_VIDEO
        self.__endpoint_signals = PartialEndpoint.RC_SIGNALS_VIDEO

    def create_request(self, message: MDFParserMessage) -> Request:
        """
        A MDFParser message

        Args:
            message (CHCMessage): _description_

        Raises:
            UnexpectedContainerMessage: _description_

        Returns:
            Request: _description_
        """
        if isinstance(message.body, SignalsProcessingResult):
            return self.__request_factory.generate_request_from_artifact_with_file(
                self.__endpoint_signals, message.body, message.body.s3_path)
        if isinstance(message.body, IMUProcessingResult):
            return self.__request_factory.generate_request_from_artifact_with_file(
                self.__endpoint_imu, message.body, message.body.s3_path)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a MDF message")
