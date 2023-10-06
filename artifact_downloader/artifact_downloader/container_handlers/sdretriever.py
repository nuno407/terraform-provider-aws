"""SDR container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, SignalsArtifact
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SDRetrieverMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class SDRContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """SDR container handler"""

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__endpoint_video = PartialEndpoint.RC_VIDEO
        self.__endpoint_snapshot = PartialEndpoint.RC_SNAPSHOT
        self.__endpoint_snapshot_signals = PartialEndpoint.RC_SIGNALS_SNAPSHOT

    def create_request(self, message: SDRetrieverMessage) -> Request:
        if isinstance(message.body, S3VideoArtifact):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_video, message.body)
        if isinstance(message.body, SnapshotArtifact):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_snapshot, message.body)
        if isinstance(message.body, SignalsArtifact) and isinstance(message.body.referred_artifact, SnapshotArtifact):
            return self.__api_request_factory.generate_request_from_artifact_with_file(
                self.__endpoint_snapshot_signals, message.body, message.body.s3_path)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a SDR message")
