""" snapshot module """
import logging as log
from datetime import datetime

from kink import inject
import pytz

from base.model.artifacts import Artifact, SnapshotArtifact
from sdretriever.constants import FileExtension
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.models import S3ObjectDevcloud, RCCS3SearchParams
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.models import S3ObjectRCC
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class SnapshotIngestor(Ingestor):
    """ Snapshot ingestor """

    def __init__(
            self,
            s3_downloader_uploader: S3DownloaderUploader,
            s3_chunk_downloader: RCCChunkDownloader) -> None:
        self.__s3_interface = s3_downloader_uploader
        self.__s3_chunk_downloader = s3_chunk_downloader
        self.__file_extension = FileExtension.SNAPSHOT

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

    def ingest(self, artifact: Artifact) -> None:
        """ Ingests a snapshot artifact """
        # validate that we are parsing a SnapshotArtifact
        if not isinstance(artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngestor can only ingest a SnapshotArtifact")

        # Download data
        params = RCCS3SearchParams(
            device_id=artifact.device_id,
            tenant=artifact.tenant_id,
            start_search=artifact.timestamp,
            stop_search=datetime.now(
                tz=pytz.UTC))

        downloaded_object: S3ObjectRCC = self.__s3_chunk_downloader.download_by_file_name(
            file_names=[artifact.uuid], search_params=params)[0]

        # Initialize file name and path
        snap_name = f"{artifact.artifact_id}{self.__file_extension}"

        # Upload files to DevCloud
        devcloud_object = S3ObjectDevcloud(
            data=downloaded_object.data,
            filename=snap_name,
            tenant=artifact.tenant_id)

        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", path_uploaded)
        artifact.s3_path = path_uploaded
