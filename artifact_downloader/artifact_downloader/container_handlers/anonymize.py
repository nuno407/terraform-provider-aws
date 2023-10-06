"""Anonymize container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import AnonymizationResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import AnonymizeMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class AnonymizeContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """Anonymize container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__api_endpoint_snap = PartialEndpoint.RC_PIPELINE_ANON_SNAPSHOT
        self.__api_endpoint_video = PartialEndpoint.RC_PIPELINE_ANON_VIDEO

    def create_request(self, message: AnonymizeMessage) -> Request:
        """
        Create a request based on the anonymized message

        Args:
            message (AnonymizeMessage): anonymized message

        Raises:
            UnexpectedContainerMessage: If the message is not an anonymization result

        Returns:
            Request: The request to be made to the artifact API
        """
        if not isinstance(message.body, AnonymizationResult):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not an anonymize message")

        if message.body.raw_s3_path.endswith(".jpeg"):
            return self.__api_request_factory.generate_request_from_artifact(self.__api_endpoint_snap, message.body)

        if message.body.raw_s3_path.endswith(".mp4"):
            return self.__api_request_factory.generate_request_from_artifact(self.__api_endpoint_video, message.body)

        raise UnexpectedContainerMessage("Anonymization result is neither a snapshot or video")
