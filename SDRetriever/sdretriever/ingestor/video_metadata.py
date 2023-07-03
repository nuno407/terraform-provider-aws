"""metacontent module"""
import json
import logging as log
import re
from operator import itemgetter

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, SignalsArtifact
from sdretriever.constants import VIDEO_CHUNK_REGX, FileExt
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3_finder_rcc import S3FinderRCC

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class VideoMetadataIngestor(MetacontentIngestor):  # pylint: disable=too-few-public-methods
    """ metadata ingestor """

    def __init__(self,
                 container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 s3_controller: S3Controller,
                 s3_finder: S3FinderRCC,
                 metadata_merger: MetadataMerger):
        super().__init__(container_services, rcc_s3_client_factory,
                         s3_controller, s3_finder, VIDEO_CHUNK_REGX)
        self.__metadata_merger = metadata_merger

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [".json.zip"]

    def __upload_mdf_data(self, source_data: dict, artifact: Artifact) -> str:
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
            self._container_svcs.raw_s3,
            s3_folder,
            FileExt.METADATA.value)

        return self._upload_metacontent_to_devcloud(upload_file)

    def ingest(self, artifact: Artifact) -> None:
        # validate that we are parsing a SignalsArtifact
        if not isinstance(artifact, SignalsArtifact):
            raise ValueError("SignalsIngestor can only ingest a SignalsArtifact")

        is_complete, chunk_paths = self._check_allparts_exist(artifact)
        if not is_complete:
            message = "Metadata is not complete, skipping for now"
            _logger.info(message)
            raise UploadNotYetCompletedError(message)
        # Fetch metadata chunks from RCC S3
        chunks: list[MetacontentChunk] = self._get_metacontent_chunks(
            chunk_paths)
        if not chunks:
            _logger.warning(
                "Could not find any metadata files for %s",
                artifact.artifact_id)
        _logger.debug("Ingesting %d Metadata chunks", len(chunks))

        # Process the raw metadata into MDF (fields 'resolution', 'chunk', 'frame', 'chc_periods')
        mdf_chunks = self.__metadata_merger.convert_metachunk_to_mdf(chunks)
        resolution, pts, frames = self.__metadata_merger.process_chunks_into_mdf(mdf_chunks)

        # Build source file to be stored - 'source_data' is the MDF
        source_data = {
            "resolution": resolution,
            "chunk": pts,
            "frame": frames
        }
        mdf_s3_path = self.__upload_mdf_data(
            source_data, artifact)

        # Store the MDF path in the artifact
        artifact.s3_path = mdf_s3_path
