"""Module responsible for merging metadata"""
import json
import logging

from operator import itemgetter
from typing import Any

from sdretriever.ingestor.metacontent import MetacontentChunk
_logger = logging.getLogger("SDRetriever." + __name__)


class MetadataMerger():
    """
    Class responsible for merging the metadata coming from RCC.

    To be done later:
        Make "convert_metachunk_to_mdf" and "convert_metachunk_to_mdf" private
        so every metadata is generated with the same format and provide a better speration of concearns.

    """

    @staticmethod
    def __json_raise_on_duplicates(ordered_pairs):
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

    @staticmethod
    def process_chunks_into_mdf(chunks: list[dict]) -> tuple[str, dict, list]:
        """Extract metadata from raw chunks and transform it into MDF data

        Args:
            chunks (list[dict]): list of chunks

        Returns:
            resolution (str): Video resolution
            pts (dict): Video timebounds, as partial timestamps and as UTC
            mdf_data(list): list with frame metadata, sorted by relative index
        """

        # this enables support for metadata version 0.4.2 from new IVS
        pts_key = "chunk" if "chunk" in chunks[0] else "chunkPts"
        utc_key = "chunk" if "chunk" in chunks[0] else "chunkUtc"

        # Calculate the bounds for partial timestamps
        # the start of the earliest and the end of the latest
        starting_chunk_time_pts = min(
            int(chunk[pts_key]["pts_start"]) for chunk in chunks)
        starting_chunk_time_utc = min(
            int(chunk[utc_key]["utc_start"]) for chunk in chunks)
        ending_chunk_time_pts = max(
            int(chunk[pts_key]["pts_end"]) for chunk in chunks)
        ending_chunk_time_utc = max(
            int(chunk[utc_key]["utc_end"]) for chunk in chunks)
        pts = {
            "pts_start": starting_chunk_time_pts,
            "pts_end": ending_chunk_time_pts,
            "utc_start": starting_chunk_time_utc,
            "utc_end": ending_chunk_time_utc
        }

        # Resolution is the same for the entire video
        resolution = chunks[0]["resolution"]

        # Build sorted list with all frame metadata, sorted
        frames = []
        for chunk in chunks:
            if chunk.get("frame"):
                for frame in chunk["frame"]:
                    frames.append(frame)
            else:
                _logger.warning("No frames in metadata chunk -> %s", chunk)

        # Sort frames by number
        mdf_data = sorted(frames, key=lambda x: int(itemgetter("number")(x)))

        return resolution, pts, mdf_data

    @staticmethod
    def convert_metachunk_to_mdf(metachunks: list[MetacontentChunk]) -> list[dict]:
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
                                        object_pairs_hook=MetadataMerger.__json_raise_on_duplicates)
            if "frame" in converted_json:
                non_empty_frames = list(
                    filter(
                        lambda frame: len(frame) > 0,
                        converted_json["frame"]))
                converted_json["frame"] = non_empty_frames
                formated_chunks.append(converted_json)

        return formated_chunks

    @staticmethod
    def merge_metadata_chunks(metachunks: list[MetacontentChunk]) -> dict[str, Any]:
        """
        Merge the metadata chunks and returns the original format but with multiple frames.

        Args:
            metachunks (list[MetacontentChunk]): The metachunks downloaded

        Returns:
            list[dict]: The metadata merged
        """

        json_chunks: list[dict] = MetadataMerger.convert_metachunk_to_mdf(
            metachunks)
        resolution, pts, mdf_data = MetadataMerger.process_chunks_into_mdf(
            json_chunks)

        if len(json_chunks) == 0:
            return {}

        first_chunk = json_chunks[0].copy()
        first_chunk["resolution"] = resolution
        first_chunk["chunkPts"] = {
            "pts_start": str(pts["pts_start"]),
            "pts_end": str(pts["pts_end"])
        }
        first_chunk["chunkUtc"] = {
            "utc_start": str(pts["utc_start"]),
            "utc_end": str(pts["utc_end"])
        }
        first_chunk["frame"] = mdf_data

        return first_chunk
