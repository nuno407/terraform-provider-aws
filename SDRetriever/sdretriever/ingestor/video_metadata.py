"""metacontent module"""
import json
import pytz
import logging as log
from kink import inject
from datetime import datetime

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, SignalsArtifact
from sdretriever.constants import FileExt
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.models import ChunkDownloadParams, S3ObjectDevcloud
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3_downloader_uploader import S3DownloaderUploader

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class VideoMetadataIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """ metadata ingestor """

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 metadata_merger: MetadataMerger,
                 s3_interface: S3DownloaderUploader):
        self.__metadata_merger = metadata_merger
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__s3_interface = s3_interface

    def __upload_metadata(self, source_data: dict, artifact: Artifact) -> str:
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            message (Artifact): Message object

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False).encode('UTF-8'))
        filename = artifact.artifact_id + FileExt.METADATA.value

        devcloud_object = S3ObjectDevcloud(
            data=source_data_as_bytes,
            filename=filename,
            tenant=artifact.tenant_id)
        return self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

    def ingest(self, artifact: Artifact) -> None:
        # validate that we are parsing a SignalsArtifact
        if not isinstance(artifact, SignalsArtifact):
            raise ValueError("SignalsIngestor can only ingest a SignalsArtifact")

        params = ChunkDownloadParams(
            recorder=artifact.referred_artifact.recorder,
            recording_id=artifact.referred_artifact.recording_id,
            chunk_ids=artifact.referred_artifact.chunk_ids,
            device_id=artifact.device_id,
            tenant=artifact.tenant_id,
            start_search=artifact.referred_artifact.timestamp,
            stop_search=datetime.now(tz=pytz.UTC))

        downloaded_chunks = self.__s3_chunk_ingestor.download_files(params)
        mdf_chunks = self.__metadata_merger.merge_metadata_chunks(downloaded_chunks)
        mdf_s3_path = self.__upload_metadata(mdf_chunks, artifact)

        # Store the MDF path in the artifact
        artifact.s3_path = mdf_s3_path
