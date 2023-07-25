"""metacontent module"""
import logging as log
from datetime import datetime

from kink import inject

from sdretriever.models import S3ObjectDevcloud, ChunkDownloadParams
from base.model.artifacts import Artifact, MetadataArtifact, SnapshotArtifact
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.constants import FileExt
from sdretriever.exceptions import S3FileNotFoundError
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
import pytz
import re

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class SnapshotMetadataIngestor:  # pylint: disable=too-few-public-methods
    """ snapshot's metadata ingestor """

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 s3_interface: S3DownloaderUploader,
                 ):
        self.__s3_interface = s3_interface
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__chunk_id_pattern = re.compile(
            r"^[^\W_]+_[^\W_]+-[a-z0-9\-]+_(\d+)\..*$")

    def __get_chunk_id(self, chunk_artifact: SnapshotArtifact) -> int:
        match = re.match(self.__chunk_id_pattern, chunk_artifact.uuid)
        if not match:
            raise ValueError(f"The chunk cannot ({chunk_artifact.uuid}) be interpreted")

        return int(match.group(1))


    def ingest(self, artifact: Artifact) -> None:
        """ Ingests a snapshot artifact """
        # validate that we are parsing a SnapshotArtifact
        if not isinstance(artifact, MetadataArtifact):
            raise ValueError("SnapshotIngestor can only ingest a MetadataArtifact")
        if not isinstance(artifact.referred_artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngeArtifact can only ingest snapshot related metadata")

        # Initialize file name and path
        metadata_snap_name = f"{artifact.artifact_id}{FileExt.METADATA.value}"
        chunk_id = self.__get_chunk_id()

        # Download data
        params = ChunkDownloadParams(recorder =artifact.referred_artifact.recorder, recording_id=artifact.referred_artifact.artifact_id, chunk_ids={chunk_id},device_id=artifact.device_id, tenant=artifact.tenant_id,start_search=artifact.referred_artifact.timestamp,
            stop_search=datetime.now(
                tz=pytz.UTC),
                suffixes=[".json.zip",".json"])


        downloaded_object = self.__s3_chunk_ingestor.download_by_chunk_id(params=params)

        if len(downloaded_object) > 1 or len(downloaded_object) == 0:
            raise S3FileNotFoundError(f"A total of {len(downloaded_object)} files were found instead of 1")

        # Upload files to DevCloud
        devcloud_object = S3ObjectDevcloud(
            data=downloaded_object[0].data,
            filename=metadata_snap_name,
            tenant=artifact.tenant_id)

        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", path_uploaded)
        artifact.s3_path = path_uploaded

