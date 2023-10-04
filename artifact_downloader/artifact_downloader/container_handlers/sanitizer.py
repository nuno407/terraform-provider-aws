from requests import Request
from typing import Union
from base.model.artifacts import OperatorArtifact, EventArtifact
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SanitizerMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoints
from kink import inject
@inject
class SanitizerContainerHandler(ContainerHandler):

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__endpoint_events = PartialEndpoints.RC_EVENT
        self.__endpoint_operator = PartialEndpoints.RC_OPERATOR


    def create_request(self, message: SanitizerMessage) -> Request:
        """
        Create a request based on an MDF message

        Args:
            message (SanitizerMessage): mdf message

        Raises:
            UnexpectedContainerMessage: If the message cannot be handled

        Returns:
            Request: The request to be made to the artifact API
        """
        if isinstance(OperatorArtifact, message.body):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_operator, message.body)
        elif  isinstance(EventArtifact, message.body) or isinstance(OperatorArtifact, message.body):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_events, message.body)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a sanitizer message")
