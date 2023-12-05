"""Selector handler"""
from requests import Request
from kink import inject
from base.model.artifacts.upload_rule_model import VideoUploadRule, SnapshotUploadRule
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SelectorMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import PartialEndpoint, RequestFactory


@inject
class SelectorContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """Selector container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory

    def create_request(self, message: SelectorMessage) -> Request:
        if isinstance(message.body, VideoUploadRule):
            return self.__api_request_factory.generate_request_from_artifact(
                PartialEndpoint.RC_VIDEO_UPLOAD_RULE, message.body)
        if isinstance(message.body, SnapshotUploadRule):
            return self.__api_request_factory.generate_request_from_artifact(
                PartialEndpoint.RC_SNAPSHOT_UPLOAD_RULE, message.body)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a Selector message")
