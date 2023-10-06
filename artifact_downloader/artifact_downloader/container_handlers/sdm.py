"""SDM container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import ProcessingResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SDMMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class SDMContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """SDM container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__api_endpoint_status = PartialEndpoint.RC_PIPELINE_STATUS

    def create_request(self, message: SDMMessage) -> Request:
        """
        Create a request based on the sdm message

        Args:
            message (SDMMessage): SDM message

        Raises:
            UnexpectedContainerMessage: If the message is not an SDMMessage

        Returns:
            Request: The request to be made to the artifact API
        """
        if not isinstance(message.body, ProcessingResult):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not an SDM message")

        return self.__api_request_factory.generate_request_from_artifact(self.__api_endpoint_status, message.body)
