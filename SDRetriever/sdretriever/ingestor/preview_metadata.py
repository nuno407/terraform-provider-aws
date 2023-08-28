"""metacontent module"""
import json
import re
import logging as log
from datetime import datetime
from kink import inject

from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.models import S3ObjectDevcloud, ChunkDownloadParamsByPrefix
from base.model.artifacts import Artifact, PreviewSignalsArtifact
import pytz

from typing import cast, Generator

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class PreviewMetadataIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """Preview metadata ingestor"""

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 s3_interface: S3DownloaderUploader,
                 metadata_merger: MetadataMerger):
        self.__s3_interface = s3_interface
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__metadata_merger = metadata_merger

    def __upload_preview_metadata(self, source_data: dict, artifact: Artifact) -> str:
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            video_msg (SignalsArtifact): Message object

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """

        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False).encode('UTF-8'))

        obj = S3ObjectDevcloud(
            data=source_data_as_bytes,
            filename=artifact.artifact_id + ".json",
            tenant=artifact.tenant_id)
        return self.__s3_interface.upload_to_devcloud_tmp(obj)

    def ingest(self, artifact: Artifact) -> None:
        """
        Ingest a preview metadata.

        Args:
            artifact (Artifact): The preview metadata artifact

        Raises:
            ValueError: If is not a PreviewSignalsArtifact.
        """
        if not isinstance(artifact, PreviewSignalsArtifact):
            raise ValueError("PreviewMetadataIngestor can only ingest a PreviewSignalsArtifact")

        preview_artifact = cast(PreviewSignalsArtifact, artifact)
        chunks_uuids = [chunk.uuid for chunk in preview_artifact.referred_artifact.chunks]

        _logger.info("Found %d chunks in message", len(chunks_uuids))
        # Download data
        params = ChunkDownloadParamsByPrefix(
            device_id=preview_artifact.device_id,
            tenant=preview_artifact.tenant_id,
            start_search=preview_artifact.referred_artifact.timestamp,
            stop_search=datetime.now(
                tz=pytz.UTC),
            suffixes=[
                ".json.zip",
                ".json"],
            files_prefix=chunks_uuids)

        downloaded_objects = self.__s3_chunk_ingestor.download_by_prefix_suffix(params=params)
        metadata = self.__metadata_merger.merge_metadata_chunks(downloaded_objects)  # type: ignore
        artifact.s3_path = self.__upload_preview_metadata(metadata, artifact)
