from requests import Request
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, SignalsArtifact
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SDRetrieverMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from kink import inject
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.request_factory import RequestFactory, PartialEndpoints


@inject
class SDRContainerHandler(ContainerHandler):

    def __init__(self, request_factory: RequestFactory):
        """
        Constructor

        Args:
            request_factory (RequestFactory): RequestFactory to create the request
        """
        self.__api_request_factory = request_factory
        self.__endpoint_video = PartialEndpoints.RC_VIDEO
        self.__endpoint_snapshot = PartialEndpoints.RC_SNAPSHOT
        self.__endpoint_snapshot_signals = PartialEndpoints.RC_SIGNALS_SNAPSHOT

    def create_request(self, message: SDRetrieverMessage) -> Request:
        if isinstance(S3VideoArtifact, message.body):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_video, message.body)
        elif  isinstance(SnapshotArtifact, message.body):
            return self.__api_request_factory.generate_request_from_artifact(self.__endpoint_snapshot, message.body)
        elif  isinstance(SignalsArtifact, message.body) and isinstance(SnapshotArtifact,message.body.referred_artifact):
            return self.__api_request_factory.generate_request_from_artifact_with_file(self.__endpoint_snapshot_signals, message.body, message.body.s3_path)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a SDR message")
