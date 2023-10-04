from requests import Request
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact, SignalsArtifact
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import SDRetrieverMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from kink import inject
from base.aws.s3 import S3Controller
@inject
class SDRContainerHandler(ContainerHandler):

    def __init__(self, s3_controller: S3Controller):
        self.__s3_controller = s3_controller

    def create_request(self, message: SDRetrieverMessage) -> Request:
        if isinstance(S3VideoArtifact, message.body):
            return self.__handle_video_artifact(message.body)
        elif  isinstance(SnapshotArtifact, message.body):
            return self.__handle_snapshot_artifact(message.body)
        elif  isinstance(SignalsArtifact, message.body) and isinstance(SnapshotArtifact,message.body.referred_artifact):
            return self.__handle_snapshot_signals_artifact(message.body)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a SDR message")


    def __handle_snapshot_signals_artifact(self, message: SignalsArtifact) -> Request:
        pass

    def __handle_snapshot_artifact(self, message: SnapshotArtifact) -> Request:
        pass

    def __handle_video_artifact(self, message: S3VideoArtifact) -> Request:
        pass
