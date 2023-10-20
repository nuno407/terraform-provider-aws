"""CHC container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import CHCResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import CHCMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class CHCContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """CHC container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Cosntructor

        Args:
            request_factory (RequestFactory): factory_request
            s3_controller (S3Downloader): s3_controller
        """
        self.__request_factory = request_factory
        self.__endpoint_video = PartialEndpoint.RC_PIPELINE_CHC_VIDEO

    def create_request(self, message: CHCMessage) -> Request:
        """
        A CHC message

        Args:
            message (CHCMessage): _description_

        Raises:
            UnexpectedContainerMessage: _description_

        Returns:
            Request: _description_
        """
        if not isinstance(message.body, CHCResult):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not chc message")

        if message.body.raw_s3_path.endswith(".mp4"):
            return self.__request_factory.generate_request_from_artifact_with_file(
                self.__endpoint_video, message.body, message.body.s3_path)

        raise UnexpectedContainerMessage("Anonymization result is neither a snapshot or video")
