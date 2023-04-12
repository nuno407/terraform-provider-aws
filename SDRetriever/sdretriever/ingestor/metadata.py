# type: ignore
"""metacontent module"""
import json
import logging as log
from operator import itemgetter

from sdretriever.constants import METADATA_FILE_EXT
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.message.video import VideoMessage

LOGGER = log.getLogger("SDRetriever." + __name__)


class MetadataIngestor(MetacontentIngestor):
    """ metadata ingestor """

    @ staticmethod
    def _json_raise_on_duplicates(ordered_pairs):
        """Convert duplicate keys to JSON array or if JSON objects, merges them."""
        result = {}
        for (key, value) in ordered_pairs:
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    for (sub_k, sub_v) in value.items():
                        result[key][sub_k] = sub_v
                elif isinstance(result[key], list):
                    result[key].append(value)
                else:
                    result[key] = [result[key], value]
            else:
                result[key] = value
        return result

    def __convert_metachunk_to_mdf(self, metachunks: list[MetacontentChunk]) -> dict[int, dict]:
        """
        Convert the metachunk into an object that can be read by _process_chunks_into_mdf.

        Args:
            metachunks (list[MetacontentChunk]): The metachunks downloaded

        Returns:
            dict[int,dict]: A dictionary with a sorted number as keys and a json as value
        """
        # Convert the chunks to a dict[int,json] rquired by _process_chunks_into_mdf
        formated_chunks: dict[int, dict] = {}
        for i, chunk in enumerate(metachunks):
            converted_json = json.loads(chunk.data,
                                        object_pairs_hook=self._json_raise_on_duplicates)
            # Add file name in the metadata. Legacy?
            converted_json["filename"] = chunk
            formated_chunks[i] = converted_json

        return formated_chunks

    def _process_chunks_into_mdf(self, chunks):
        """Extract metadata from raw chunks and transform it into MDF data

        Args:
            chunks (dict): Dictionary of chunks, indexed by their relative order

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
            [int(chunks[id][pts_key]['pts_start']) for id in chunks.keys()])
        starting_chunk_time_utc = min(
            [int(chunks[id][utc_key]['utc_start']) for id in chunks.keys()])
        ending_chunk_time_pts = max(
            [int(chunks[id][pts_key]['pts_end']) for id in chunks.keys()])
        ending_chunk_time_utc = max(
            [int(chunks[id][utc_key]['utc_end']) for id in chunks.keys()])
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
            if chunks[chunk].get('frame'):
                for frame in chunks[chunk]["frame"]:
                    frames.append(frame)
            else:
                LOGGER.warning("No frames in metadata chunk -> %s", chunks[chunk])

        # Sort frames by number
        mdf_data = sorted(frames, key=lambda x: int(itemgetter("number")(x)))

        return resolution, pts, mdf_data

    def _upload_source_data(self, source_data, video_msg, video_id: str) -> str:
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            video_msg (VideoMessage): Message object

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """

        s3_folder = video_msg.tenant + "/"
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        upload_file = MetacontentDevCloud(
            source_data_as_bytes,
            video_id,
            self.container_svcs.raw_s3,
            s3_folder,
            METADATA_FILE_EXT)

        return self._upload_metacontent_to_devcloud(upload_file)

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [".json.zip"]

    def ingest(self, video_msg: VideoMessage, video_id: str, metadata_chunk_paths: set[str]):
        # Fetch metadata chunks from RCC S3
        chunks: list[MetacontentChunk] = self._get_metacontent_chunks(video_msg, metadata_chunk_paths)
        if not chunks:
            return False

        LOGGER.debug("Ingesting %d Metadata chunks", len(chunks))

        # Process the raw metadata into MDF (fields 'resolution', 'chunk', 'frame', 'chc_periods')
        resolution, pts, frames = self._process_chunks_into_mdf(self.__convert_metachunk_to_mdf(chunks))

        # Build source file to be stored - 'source_data' is the MDF, extended with
        # the original queue message and its identifier
        source_data = {
            "messageid": video_msg.messageid,
            "message": video_msg.raw_message,
            "resolution": resolution,
            "chunk": pts,
            "frame": frames
        }
        mdf_s3_path = self._upload_source_data(
            source_data, video_msg, video_id)

        if mdf_s3_path:
            return f"s3://{self.container_svcs.raw_s3}/{mdf_s3_path}"
        else:
            return False
