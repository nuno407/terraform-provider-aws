"""metacontent module"""
import logging as log
from datetime import datetime
import pytz

from kink import inject

from sdretriever.models import S3ObjectDevcloud, ChunkDownloadParamsByPrefix
from base.model.artifacts import Artifact, SignalsArtifact, SnapshotArtifact
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.constants import FileExtension
from sdretriever.exceptions import EmptyFileError
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class SnapshotMetadataIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """ snapshot's metadata ingestor """

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 s3_interface: S3DownloaderUploader,
                 ):
        self.__s3_interface = s3_interface
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__file_extension = FileExtension.METADATA

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
        if not isinstance(artifact, SignalsArtifact):
            raise ValueError("SnapshotIngestor can only ingest a MetadataArtifact")
        if not isinstance(artifact.referred_artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngeArtifact can only ingest snapshot related metadata")

        # Download data
        params = ChunkDownloadParamsByPrefix(
            device_id=artifact.device_id,
            tenant=artifact.tenant_id,
            start_search=artifact.referred_artifact.timestamp,
            stop_search=datetime.now(
                tz=pytz.UTC),
            suffixes=[
                ".json.zip",
                ".json"],
            files_prefix=[
                artifact.referred_artifact.uuid])

        downloaded_object = self.__s3_chunk_ingestor.download_by_prefix_suffix(params=params)

        # Workarround for error https://rb-tracker.bosch.com/tracker13/browse/MC-50263
        if len(downloaded_object[0].data) == 0:
            raise EmptyFileError("Snapshot metadata is empty")

        # Initialize file name and path
        metadata_snap_name = f"{artifact.artifact_id}{self.__file_extension}"

        # Upload files to DevCloud
        devcloud_object = S3ObjectDevcloud(
            data=downloaded_object[0].data,
            filename=metadata_snap_name,
            tenant=artifact.tenant_id)

        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", path_uploaded)
        artifact.s3_path = path_uploaded
