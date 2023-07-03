"""Module for ingesting video artifacts from RCC S3"""
from botocore.errorfactory import ClientError
from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, Resolution, S3VideoArtifact
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import FileExt
from sdretriever.exceptions import (FileAlreadyExists, S3DownloadError,
                                    S3UploadError)
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.ingestor.post_processor import IVideoPostProcessor
from sdretriever.s3_finder import S3Finder


@inject
class S3VideoIngestor(Ingestor):
    """Ingestor for video artifacts stored on RCC S3"""

    def __init__(self, container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 config: SDRetrieverConfig,
                 s3_controller: S3Controller,
                 post_processor: IVideoPostProcessor,
                 s3_finder: S3Finder):
        super().__init__(container_services, rcc_s3_client_factory, s3_finder,
                         s3_controller)  # pylint: disable=no-value-for-parameter, missing-positional-arguments
        self._config = config
        self._s3_controller = s3_controller
        self._post_processor = post_processor

    def __get_video_s3(self, artifact: S3VideoArtifact) -> bytes:
        """Gets video file from RCC S3"""
        # download video from RCC S3
        bucket, key = S3Controller.get_s3_path_parts(artifact.rcc_s3_path)
        rcc_s3_controller = S3Controller(self._rcc_s3_client_factory())
        try:
            video_bytes = rcc_s3_controller.download_file(bucket, key)
        except ClientError as err:
            raise S3DownloadError(f"Failed to download video {key} from RCC S3") from err

        return video_bytes

    def ingest(self, artifact: Artifact):
        """Obtain video from RCC S3 and upload to our S3"""
        # validate that we are dealing with the correct artifact type
        if not isinstance(artifact, S3VideoArtifact):
            raise ValueError("S3VideoIngestor can only ingest an S3VideoArtifact")

        upload_path = f"{artifact.tenant_id}/{artifact.artifact_id}{FileExt.VIDEO.value}"
        # check if the video is already uploaded
        if self._config.discard_video_already_ingested:
            exists_on_devcloud = self._s3_controller.check_s3_file_exists(
                self._container_svcs.raw_s3, upload_path)

            if exists_on_devcloud:
                raise FileAlreadyExists(
                    f"Video {upload_path} already exists on DevCloud, message will be skipped")

        # download video from RCC S3
        video_bytes = self.__get_video_s3(artifact)

        # upload video to DevCloud S3
        upload_path = self._upload_file(upload_path, video_bytes)

        # post process video
        video_info = self._post_processor.execute(video_bytes)
        artifact.resolution = Resolution(video_info.width, video_info.height)
        artifact.actual_duration = video_info.duration
        artifact.s3_path = upload_path
