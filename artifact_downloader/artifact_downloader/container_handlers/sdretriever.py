"""SDR container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, SignalsArtifact
from base.model.metadata.api_messages import SnapshotSignalsData
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.message.incoming_messages import SDRetrieverMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class SDRContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """SDR container handler"""

    def __init__(self, request_factory: RequestFactory, s3_downloader: S3Downloader):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__endpoint_video = PartialEndpoint.RC_VIDEO
        self.__endpoint_snapshot = PartialEndpoint.RC_SNAPSHOT
        self.__endpoint_snapshot_signals = PartialEndpoint.RC_SIGNALS_SNAPSHOT
        self.__s3_downloader = s3_downloader

    def create_request(self, message: SDRetrieverMessage) -> Request:
        if isinstance(message.body, S3VideoArtifact):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_video, message.body)
        if isinstance(message.body, SnapshotArtifact):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_snapshot, message.body)
        if isinstance(message.body, SignalsArtifact) and isinstance(message.body.referred_artifact, SnapshotArtifact):
            downloaded_file = self.__s3_downloader.download_convert_json(message.body.referred_artifact.s3_path)
            model = SnapshotSignalsData.model_validate({"data":downloaded_file,"message":message.body})
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_snapshot_signals, model)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a SDR message")
