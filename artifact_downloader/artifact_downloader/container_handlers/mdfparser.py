"""MDFParser container handler"""
from requests import Request
from kink import inject
from base.model.artifacts import IMUDataArtifact, IMUProcessingResult, SignalsProcessingResult, VideoSignalsData
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import MDFParserMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint
from artifact_downloader.s3_downloader import S3Downloader


@inject
class MDFParserContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """MDFParser container handler"""

    def __init__(self, request_factory: RequestFactory, s3_downloader: S3Downloader):
        """
        Cosntructor

        Args:
            request_factory (RequestFactory): factory_request
        """
        self.__request_factory = request_factory
        self.__s3_downloader = s3_downloader

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
            data = self.__s3_downloader.download_convert_json(message.body.s3_path)
            artifact = VideoSignalsData(
                data=data,
                aggregated_metadata=message.body.recording_overview,
                correlation_id=message.body.correlation_id,
                tenant_id=message.body.tenant_id,
                video_raw_s3_path=message.body.video_raw_s3_path
            )
            return self.__request_factory.generate_request_from_artifact(
                self.__endpoint_signals, artifact)

        if isinstance(message.body, IMUProcessingResult):
            # if the model validation causes a bottleneck, create a dict payload and use generate_request instead
            data = self.__s3_downloader.download_convert_json(message.body.s3_path)
            artifact = IMUDataArtifact(data=data, message=message.body)
            return self.__request_factory.generate_request_from_artifact(
                self.__endpoint_imu, artifact)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a MDF message")
