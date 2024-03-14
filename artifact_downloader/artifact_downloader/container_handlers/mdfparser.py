"""MDFParser container handler"""
from base64 import b64encode
import pandas as pd
from requests import Request
from kink import inject
from base.model.artifacts import IMUDataArtifact, IMUProcessingResult, SignalsProcessingResult, VideoSignalsData
from artifact_downloader.memory_buffer import UnclosableMemoryBuffer
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
            data_df: pd.DataFrame = self.__s3_downloader.download_convert_json_pandas(message.body.s3_path)

            # Covert the pandas dataframe to parquet and compress it
            buffer = UnclosableMemoryBuffer()
            data_df.to_parquet(buffer, engine="fastparquet", index=False, compression="gzip")
            del data_df

            # Encodes the buffer to base64
            buffer.seek(0)
            encoded_imu = b64encode(buffer.read()).decode("utf-8")
            del buffer

            imu_artifact = IMUDataArtifact(
                message=message.body,
                data=encoded_imu
            )
            return self.__request_factory.generate_request_from_artifact(
                self.__endpoint_imu, imu_artifact)

        raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not a MDF message")
