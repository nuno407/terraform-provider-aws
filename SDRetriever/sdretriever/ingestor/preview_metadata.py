"""metacontent module"""
import json
import re
import logging as log
from datetime import datetime
from pytz import UTC

from kink import inject
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.s3_finder_rcc import S3FinderRCC
from sdretriever.constants import FileExt
from sdretriever.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.exceptions import UploadNotYetCompletedError, S3FileNotFoundError
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.models import RCCS3SearchParams

from sdretriever.config import SDRetrieverConfig
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, PreviewSignalsArtifact
from base.aws.model import S3ObjectInfo

from typing import cast, Optional

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class PreviewMetadataIngestor(MetacontentIngestor):  # pylint: disable=too-few-public-methods
    """Preview metadata ingestor"""

    def __init__(self,
                 container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 s3_controller: S3Controller,
                 s3_crawler: S3CrawlerRCC,
                 s3_finder: S3FinderRCC,
                 sdr_config: SDRetrieverConfig,
                 metadata_merger: MetadataMerger):
        super().__init__(container_services, rcc_s3_client_factory,
                         s3_controller, s3_finder, re.compile(".*"))
        self.__config = sdr_config
        self.__rcc_s3_crawler = s3_crawler
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

        s3_folder = artifact.tenant_id
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False).encode('UTF-8'))
        upload_file = MetacontentDevCloud(
            source_data_as_bytes,
            artifact.artifact_id,
            self.__config.temporary_bucket,
            s3_folder,
            FileExt.METADATA.value)

        return self._upload_metacontent_to_devcloud(upload_file)

    def __download_chunks(self, chunks: list[S3ObjectInfo]) -> list[MetacontentChunk]:
        """
        Download ALL chunks from RCC and unzip them if necessary.

        Args:
            chunks (list[S3ObjectInfo]): Chunks to be downloaded.

        Raises:
            S3FileNotFoundError: If not all chunks were downloaded.

        Returns:
            list[MetacontentChunk]: A list of chunks downloaded from RCC.
        """
        metadata_chunks_keys = set(map(lambda x: x.key, chunks))
        downloaded_chunks: list[MetacontentChunk] = self._get_metacontent_chunks(
            metadata_chunks_keys)
        if len(downloaded_chunks) != len(chunks):
            raise S3FileNotFoundError("Not all metadata chunks were downloaded from RCC %d/%d",
                                      len(downloaded_chunks), len(metadata_chunks_keys))

        return downloaded_chunks

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [".json.zip"]

    def metadata_chunk_match(self, object_info: S3ObjectInfo) -> Optional[str]:
        """
        Function used to match a chunk id in RCC, it is passed to the RCC Crawler.
        If the S3 object has a metadata extension returns it's ID to be matched.

        Args:
            object_info (S3ObjectInfo): The object info passed by S3Crawler.

        Returns:
            Optional[str]: The chunk ID or None.
        """
        file_key = object_info.key
        if not (file_key.endswith(".json") or file_key.endswith(".zip")):
            return None

        return self._apply_chunk_regex(object_info.key)

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
        snapshot_names = {
            snapshot_artifact.uuid for snapshot_artifact in preview_artifact.referred_artifact.chunks}

        rcc_s3_params = RCCS3SearchParams(
            device_id=preview_artifact.device_id,
            tenant=preview_artifact.tenant_id,
            start_search=preview_artifact.timestamp,
            stop_search=datetime.now(tz=UTC))

        metadata_chunks_found: dict[str, S3ObjectInfo] = self.__rcc_s3_crawler.search_files(
            snapshot_names,
            rcc_s3_params,
            match_to_file=self.metadata_chunk_match)

        # Check if not all chunks were found
        if len(metadata_chunks_found) != len(snapshot_names):
            found_metadata_filenames = set(metadata_chunks_found.keys())
            _logger.debug(
                "The following chunks were not found in RCC: %s",
                snapshot_names.difference(found_metadata_filenames))
            raise UploadNotYetCompletedError(f"Not all metadata chunks were found {len(metadata_chunks_found)}/{len(snapshot_names)}")

        # Fetch metadata chunks from RCC S3
        downloaded_chunks = self.__download_chunks(list(metadata_chunks_found.values()))

        # Merge the chunks
        metadata = self.__metadata_merger.merge_metadata_chunks(downloaded_chunks)

        # Upload the chunks
        mdf_s3_path = self.__upload_preview_metadata(
            metadata, artifact)

        # Store the MDF path in the artifact
        artifact.s3_path = mdf_s3_path
