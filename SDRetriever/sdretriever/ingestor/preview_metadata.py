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
from sdretriever.models import S3ObjectDevcloud, ChunkDownloadParamsByID
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
        self.__chunk_id_pattern = re.compile(
            r"^[^\W_]+_[^\W_]+-[a-z0-9\-]+_(\d+)\..*$")
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

    def __get_chunk_ids(self, object_info: PreviewSignalsArtifact) -> Generator[int, None, None]:
        """
        Strips the ids from each chunk name.

        Args:
            object_info (PreviewSignalsArtifact): The artifact to check the data

        Raises:
            ValueError: If one of the chunk names do not match the regex expression

        Yields:
            Generator[int,None,None]: The ID of each chunk
        """

        for snapshot_artifact in object_info.referred_artifact.chunks:
            match = re.match(self.__chunk_id_pattern, snapshot_artifact.uuid)
            if not match:
                raise ValueError(f"The chunk ({snapshot_artifact.uuid}) cannot be interpreted")

            yield int(match.group(1))

    def ingest(self, artifact: Artifact) -> None:
        """
        Ingest a preview metadata.

        Args:
            artifact (Artifact): The preview metadata artifact

        Raises:
            ValueError: If is not a PreviewSignalsArtifact.
            TemporaryIngestionError: If some chunks could not be found.
            S3FileNotFoundError: If a chunk could not be found in RCC.
        """
        if not isinstance(artifact, PreviewSignalsArtifact):
            raise ValueError("PreviewMetadataIngestor can only ingest a PreviewSignalsArtifact")

        preview_artifact = cast(PreviewSignalsArtifact, artifact)
        chunk_ids = list(self.__get_chunk_ids(preview_artifact))
        _logger.info("Found %d chunks in message", len(chunk_ids))

        # Download data
        params = ChunkDownloadParamsByID(recorder=artifact.referred_artifact.recorder,
                                         recording_id=artifact.referred_artifact.recording_id,
                                         chunk_ids=chunk_ids, device_id=artifact.device_id,
                                         tenant=artifact.tenant_id,
                                         start_search=artifact.referred_artifact.timestamp,
                                         stop_search=datetime.now(
                                             tz=pytz.UTC),
                                         suffixes=[".json.zip", ".json"])

        downloaded_objects = self.__s3_chunk_ingestor.download_by_chunk_id(params=params)
        metadata = self.__metadata_merger.merge_metadata_chunks(downloaded_objects)  # type: ignore
        artifact.s3_path = self.__upload_preview_metadata(metadata, artifact)
