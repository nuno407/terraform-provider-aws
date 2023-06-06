""" ingestor module """
import logging as log
from dataclasses import dataclass
from datetime import datetime

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.aws.shared_functions import StsHelper
from base.model.artifacts import Artifact, KinesisVideoArtifact, Resolution
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import FileExt
from sdretriever.exceptions import FileAlreadyExists, KinesisDownloadError
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.ingestor.post_processor import IVideoPostProcessor
from sdretriever.s3_finder_rcc import S3FinderRCC

_logger = log.getLogger("SDRetriever." + __name__)
STREAM_TIMESTAMP_TYPE = 'PRODUCER_TIMESTAMP'


@dataclass
class Video:
    """ Video actual timestamps """
    content_bytes: bytes
    actual_start: datetime
    actual_end: datetime


@inject
class KinesisVideoIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """ Video ingestor class """

    def __init__(
            self,
            container_services: ContainerServices,
            rcc_s3_client_factory: S3ClientFactory,
            config: SDRetrieverConfig,
            sts_helper: StsHelper,
            s3_controller: S3Controller,
            post_processor: IVideoPostProcessor,
            s3_finder: S3FinderRCC) -> None:
        super().__init__(
            container_services,
            rcc_s3_client_factory, s3_finder, s3_controller)  # pylint: disable=no-value-for-parameter, missing-positional-arguments
        self._config = config
        self._sts_helper = sts_helper
        self._s3_controller = s3_controller
        self._post_processor = post_processor

    def __get_video_kinesis(self, artifact: KinesisVideoArtifact) -> Video:
        """ Get video from KinesisVideoStream """

        # Requests credentials to assume specific cross-account role
        role_credentials = self._sts_helper.get_credentials()

        # Get clip from KinesisVideoStream
        try:
            _logger.info(
                "Downloading video clip %s from KinesisVideoStream %s",
                artifact.artifact_id,
                artifact.stream_name)

            # toggle between download from kinesis and download from S3 based on config value
            content_bytes, video_start_ts, video_end_ts = self._container_svcs.get_kinesis_clip(
                role_credentials,
                artifact.stream_name,
                artifact.timestamp,
                artifact.end_timestamp,
                STREAM_TIMESTAMP_TYPE)

        except Exception as exception:  # pylint: disable=broad-except
            _logger.exception(
                "Could not obtain Kinesis clip for %s", artifact.artifact_id)
            raise KinesisDownloadError() from exception

        return Video(content_bytes, video_start_ts, video_end_ts)

    def ingest(self, artifact: Artifact) -> None:
        """Obtain video from KinesisVideoStreams and upload it to our raw data S3"""
        # validate that we are parsing a VideoArtifact
        if not isinstance(artifact, KinesisVideoArtifact):
            raise ValueError("KinesisVideoIngestor can only ingest a KinesisVideoArtifact")

        video = self.__get_video_kinesis(artifact)

        # S3 folder path based on the tenant_id where the video will be uploaded
        s3_folder = artifact.tenant_id + "/"
        # Upload video clip into raw data S3 bucket
        s3_filename = artifact.artifact_id
        s3_path = s3_folder + s3_filename + FileExt.VIDEO.value

        if self._config.discard_video_already_ingested:
            _logger.info("Checking for the existance of %s file in the %s bucket",
                         s3_path,
                         self._container_svcs.raw_s3)
            exists_on_devcloud = self._s3_controller.check_s3_file_exists(
                self._container_svcs.raw_s3, s3_path)

            if exists_on_devcloud:
                raise FileAlreadyExists(
                    f"Video {s3_path} already exists on DevCloud, message will be skipped")

        uploaded_path = self._upload_file(s3_path, video.content_bytes)

        # Obtain video details via ffprobe and prepare data to be used
        # to generate the video's entry on the database
        video_info = self._post_processor.execute(video.content_bytes)

        # Fill the optional fields in the artifact which are now known
        artifact.actual_timestamp = video.actual_start
        artifact.actual_end_timestamp = video.actual_end
        artifact.actual_duration = video_info.duration
        artifact.resolution = Resolution(video_info.width, video_info.height)
        artifact.s3_path = uploaded_path
