"""metacontent module"""
import logging as log
from datetime import datetime

from kink import inject

from sdretriever.models import S3ObjectDevcloud, ChunkDownloadParamsByPrefix
from base.model.artifacts import Artifact, SignalsArtifact, SnapshotArtifact
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.constants import FileExt
from sdretriever.exceptions import S3FileNotFoundError
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
import pytz
import re

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

        # Initialize file name and path
        metadata_snap_name = f"{artifact.artifact_id}{FileExt.METADATA.value}"

        # Upload files to DevCloud
        devcloud_object = S3ObjectDevcloud(
            data=downloaded_object[0].data,
            filename=metadata_snap_name,
            tenant=artifact.tenant_id)

        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", path_uploaded)
        artifact.s3_path = path_uploaded
