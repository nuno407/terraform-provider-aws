"""Module for ingesting video artifacts from RCC S3"""
from botocore.errorfactory import ClientError
from kink import inject

from base.aws.s3 import S3Controller
from base.model.artifacts import Artifact, Resolution, S3VideoArtifact
from sdretriever.constants import FileExtension
from sdretriever.exceptions import S3DownloadError
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.ingestor.post_processor import IVideoPostProcessor
from sdretriever.models import S3ObjectDevcloud, S3ObjectRCC
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader


@inject
class S3VideoIngestor(Ingestor):
    """Ingestor for video artifacts stored on RCC S3"""

    def __init__(self,
                 post_processor: IVideoPostProcessor,
                 s3_interface: S3DownloaderUploader):
        self._post_processor = post_processor
        self.__s3_interface = s3_interface
        self.__file_extension = FileExtension.VIDEO

    def __get_video_s3(self, artifact: S3VideoArtifact) -> S3ObjectRCC:
        """Gets video file from RCC S3"""
        # download video from RCC S3
        bucket, key = S3Controller.get_s3_path_parts(artifact.rcc_s3_path)
        try:
            video_bytes = self.__s3_interface.download_from_rcc([key], bucket)[0]
        except (ClientError, KeyError) as err:
            raise S3DownloadError(
                f"Failed to download video {key} from {bucket} on RCC S3") from err

        return video_bytes

    def is_already_ingested(self, artifact: Artifact) -> bool:
        """
        Checks if there is already a file with the same name in DevCloud.

        Args:
            artifact (Artifact): _description_

        Returns:
            bool: True if file exists, False otherwise
        """
        return self.__s3_interface.is_file_devcloud_raw(
            artifact.artifact_id + self.__file_extension, artifact.tenant_id)

    def ingest(self, artifact: Artifact):
        """Obtain video from RCC S3 and upload to our S3"""
        # validate that we are dealing with the correct artifact type
        if not isinstance(artifact, S3VideoArtifact):
            raise ValueError("S3VideoIngestor can only ingest an S3VideoArtifact")

        # download video from RCC S3
        video_object = self.__get_video_s3(artifact)

        # upload video to DevCloud S3
        devcloud_object = S3ObjectDevcloud(
            data=video_object.data,
            filename=f"{artifact.artifact_id}{self.__file_extension}",
            tenant=artifact.tenant_id)

        # post process video
        video_info = self._post_processor.execute(video_object.data)
        upload_path = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)
        artifact.resolution = Resolution(width=video_info.width, height=video_info.height)
        artifact.actual_duration = video_info.duration
        artifact.s3_path = upload_path