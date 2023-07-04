"""metacontent module"""
import json
import logging as log
from operator import itemgetter

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, SignalsArtifact
from sdretriever.constants import FileExt
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.s3_finder import S3Finder

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class VideoMetadataIngestor(MetacontentIngestor):  # pylint: disable=too-few-public-methods
    """ metadata ingestor """

    def __init__(self,
                 container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 s3_controller: S3Controller,
                 s3_finder: S3Finder):
        super().__init__(container_services, rcc_s3_client_factory,
                         s3_controller, s3_finder)

    def __convert_metachunk_to_mdf(self, metachunks: list[MetacontentChunk]) -> list[dict]:
        """
        Convert the metachunk into an object that can be read by _process_chunks_into_mdf.

        Args:
            metachunks (list[MetacontentChunk]): The metachunks downloaded

        Returns:
            list[dict]: A list of the metachunks parsed to dict
        """
        formated_chunks: list[dict] = []
        for chunk in metachunks:
            converted_json = json.loads(chunk.data,
                                        object_pairs_hook=self._json_raise_on_duplicates)
            # Add file name in the metadata. Legacy?
            converted_json["filename"] = chunk
            formated_chunks.append(converted_json)

        return formated_chunks

    def _process_chunks_into_mdf(self, chunks: list[dict]) -> tuple[str, dict, list]:
        """Extract metadata from raw chunks and transform it into MDF data

        Args:
            chunks (list[dict]): list of chunks

        Returns:
            resolution (str): Video resolution
            pts (dict): Video timebounds, as partial timestamps and as UTC
            mdf_data(list): list with frame metadata, sorted by relative index
        """

        # this enables support for metadata version 0.4.2 from new IVS
        pts_key = 'chunk' if 'chunk' in chunks[0] else 'chunkPts'
        utc_key = 'chunk' if 'chunk' in chunks[0] else 'chunkUtc'

        # Calculate the bounds for partial timestamps
        # the start of the earliest and the end of the latest
        starting_chunk_time_pts = min(
            [int(chunk[pts_key]['pts_start']) for chunk in chunks])
        starting_chunk_time_utc = min(
            [int(chunk[utc_key]['utc_start']) for chunk in chunks])
        ending_chunk_time_pts = max(
            [int(chunk[pts_key]['pts_end']) for chunk in chunks])
        ending_chunk_time_utc = max(
            [int(chunk[utc_key]['utc_end']) for chunk in chunks])
        pts = {
            "pts_start": starting_chunk_time_pts,
            "pts_end": ending_chunk_time_pts,
            "utc_start": starting_chunk_time_utc,
            "utc_end": ending_chunk_time_utc
        }

        # Resolution is the same for the entire video
        resolution = chunks[0]['resolution']

        # Build sorted list with all frame metadata, sorted
        frames = []
        for chunk in chunks:
            if chunk.get('frame'):
                for frame in chunk["frame"]:
                    if isinstance(frame, dict) and "number" in frame:
                        frames.append(frame)
            else:
                _logger.warning("No frames in metadata chunk -> %s", chunk)

        # Sort frames by number
        mdf_data = sorted(frames, key=lambda x: int(itemgetter("number")(x)))

        return resolution, pts, mdf_data

    def _upload_source_data(self, source_data: dict, artifact: SignalsArtifact) -> str:
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            video_msg (VideoMessage): Message object

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """

        s3_folder = artifact.tenant_id + "/"
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        upload_file = MetacontentDevCloud(
            source_data_as_bytes,
            artifact.artifact_id,
            self._container_svcs.raw_s3,
            s3_folder,
            FileExt.METADATA.value)

        return self._upload_metacontent_to_devcloud(upload_file)

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [".json.zip"]

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
        mdf_chunks = self.__convert_metachunk_to_mdf(chunks)
        resolution, pts, frames = self._process_chunks_into_mdf(mdf_chunks)

        # Build source file to be stored - 'source_data' is the MDF
        source_data = {
            "resolution": resolution,
            "chunk": pts,
            "frame": frames
        }
        mdf_s3_path = self._upload_source_data(
            source_data, artifact)

        # Store the MDF path in the artifact
        artifact.s3_path = mdf_s3_path
