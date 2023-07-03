""" metacontent module """
import gzip
import logging as log
import re
import sys
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator, Optional

import pytz
from botocore.exceptions import ClientError
from kink import inject

from base.aws.container_services import ContainerServices, RCCS3ObjectParams
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import (ImageBasedArtifact, MetadataArtifact,
                                  VideoArtifact)
from sdretriever.exceptions import S3UploadError
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3_finder import S3Finder

_logger = log.getLogger("SDRetriever." + __name__)
REXP_MP4 = re.compile(r"([^\W_]+)_([^\W_]+)-([a-z0-9\-]+)_(\d+)\.mp4$")


@dataclass
class MetacontentChunk:
    """Represents the data inside a normal chunk or an IMU chunk"""
    data: bytes
    filename: str


@dataclass
class MetacontentDevCloud:
    """Represents the data that will be written into DevCloud"""
    data: bytes
    video_id: str
    bucket: str
    msp: str
    extension: str


@inject
class MetacontentIngestor(Ingestor):
    """ Metacontent ingestor """

    def __init__(self, container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory, s3_controller: S3Controller, s3_finder: S3Finder):
        super().__init__(container_services, rcc_s3_client_factory, s3_finder, s3_controller)
        self._s3_controller = s3_controller

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

    @staticmethod
    def _get_readable_size_object(_object: list[MetacontentChunk]) -> str:
        """
        Returns the size of an object in a human readable format.
        Adapted from here: https://web.archive.org/web/20111010015624/http://blogmag.net/blog/read/38/Print_human_readable_file_size

        Args:
            _object (object): _description_

        Returns:
            str: _description_
        """
        bytes_size = 0.0
        for element in _object:
            bytes_size += sys.getsizeof(element.data)

        for unit in ["Bytes", "KB", "MB", "GB", "TB"]:
            if abs(bytes_size) < 1024.0:
                return f"{bytes_size:3.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f}TB"

    def _upload_metacontent_to_devcloud(self, file: MetacontentDevCloud) -> str:
        """
        Upload a metacontent to the DevCloud.

        Args:
            file (MetacontentDevCloud): The file to be uploaded.

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            str: The path to where it got uploaded
        """
        s3_path = f"{file.msp}{file.video_id}{file.extension}"

        return self._upload_file(s3_path, file.data, file.bucket)

    def _get_chunks_lookup_paths(
            self,
            artifact: MetadataArtifact,
            start_time: Optional[datetime] = None,
            end_time: Optional[datetime] = None) -> Iterator[str]:
        """
        Get all paths to search for chunks in RCC S3 bucket between two timestamps.
        It includes the start and end hour folder.
        It returns the path with the prefixes already in place (year,month,day,hour,recorder_recorder),
        the reason why it returns the recorder is to diferentiate the InteriorRecorderPreview from the InteriorRecorder.

        Args:
            artifact (MetadataArtifact): Artifact for which to get the paths
            start_time (datetime, optional): The start date to search
                if not specified, defaults to time the related artifact started uploading.
            end_time (datetime, optional): The end date to stop search
                if not specified, defaults to time the related artifact finished uploading.

        Yields:
            Iterator[str]: An Iterator containing all possible paths
        """
        # If end time is not provided use the message upload timestamps
        if end_time is None:
            end_time = artifact.referred_artifact.upload_timing.end + timedelta(seconds=1)

        if start_time is None:
            start_time = artifact.referred_artifact.upload_timing.end

        start_time = start_time.replace(microsecond=0, second=0, minute=0)

        path = f'{artifact.tenant_id}/{artifact.device_id}/'

        paths = self._s3_finder.discover_s3_subfolders(
            path,
            self._container_svcs.rcc_info["s3_bucket"],
            self._rcc_s3_client,
            start_time,
            end_time)

        if not isinstance(artifact.referred_artifact, VideoArtifact):
            raise ValueError("Invalid referred_artifact, only VideoArtifact, accepted")

        for s3_path in paths:
            yield s3_path + f'{artifact.referred_artifact.recorder.value}_{artifact.referred_artifact.recorder.value}'

    def _get_metacontent_chunks(
            self,
            metadata_chunk_paths: set[str]) -> list[MetacontentChunk]:
        """
        Download metacontent chunks from RCC S3.
        If the the filename ends with .zip extensionm it will also decompress it and return the content within.

        Args:
            metadata_chunk_paths (set[str]): A set containing all the meta chunks
            artifact_id (str): The artifact id for logging purposes

        Returns:
            chunks (list[MetacontentChunk]): List with all raw chunks between the bounds defined, indexed by their relative order. Defaults to an empty list.
        """

        chunks: list[MetacontentChunk] = []

        # Cycle through the received list of matching files,
        # download them from S3 and store them in memory
        for file_name in metadata_chunk_paths:
            metadata_bytes = self._container_svcs.download_file(
                self._rcc_s3_client, self._container_svcs.rcc_info["s3_bucket"], file_name)

            metadata_bytes_casted = bytes(metadata_bytes)

            if file_name.endswith('.zip'):
                metadata_bytes_casted = gzip.decompress(metadata_bytes_casted)

            chunks.append(MetacontentChunk(data=metadata_bytes_casted, filename=file_name))

        _logger.debug(
            "All the chunks download have %s in memory",
            self._get_readable_size_object(chunks))

        return chunks

    def _search_chunks_in_s3_path(self,
                                  s3_path: str,
                                  bucket: str,
                                  match_chunk_extensions: list[str],
                                  start_time: datetime = datetime.min.replace(tzinfo=pytz.UTC),
                                  end_time: datetime = datetime.max.replace(tzinfo=pytz.UTC),
                                  recorder_type: Optional[str] = None) -> tuple[set[str],
                                                                                set[str]]:
        """
        Lists all metadata and video chunks for a specific path.

        If start_time and end_time fields are provided then v chunks will
        only be fetch if they were modified between start_time and end_time.

        It searches up to a maximum of 5000 objects
        Args:
            s3_path (str): Path to list the chunks.
            bucket (str): The S3 bucket name.
            match_chunk_extension (list[str]): A list with file extensions.
            start_time (datetime): Lower bound to fetch videos chunks
            end_time (datetime): Upper bound to fetch videos chunks
            recorder_type (str): The recorder type to searched for. E.g: InteriorRecorder

        Returns:
            Tuple[set[str], set[str]]: A tuple containing all metadata and video path chunks respectively.
        """
        has_files, resp = self.check_if_s3_rcc_path_exists(
            s3_path, bucket, max_s3_api_calls=5)

        metadata_chunks_set: set[str] = set()
        video_chunks_set: set[str] = set()

        if not has_files:
            return metadata_chunks_set, video_chunks_set

        # Build the regex for metachunk
        rexp_metachunks = self._build_chunks_regex(match_chunk_extensions)

        for file_entry in resp['Contents']:
            file_path: str = file_entry['Key']
            modified_date: datetime = file_entry['LastModified']

            # Avoiding catching chunks from other recording
            if recorder_type and recorder_type not in file_path:
                _logger.debug(
                    "Chunk (%s) is not of the type (%s)",
                    file_path,
                    recorder_type)
                continue

            if REXP_MP4.search(file_path):

                if modified_date < start_time:
                    _logger.debug(
                        "Ignoring chunk (%s) modified at (%s), it's under the uploadstarted datetime (%s)",
                        file_path,
                        str(modified_date),
                        str(end_time))
                    continue

                if modified_date > end_time:
                    _logger.debug(
                        "Ignoring chunk (%s) modified at (%s), it's over the uploadfinished datetime (%s)",
                        file_path,
                        str(modified_date),
                        str(end_time))
                    continue

                video_chunks_set.add(file_path)

            elif rexp_metachunks.match(file_path):
                metadata_chunks_set.add(file_path)
            else:
                _logger.warning(
                    "Ignoring chunk (%s) modified at (%s), does not match any known chunk",
                    file_path,
                    str(modified_date))

        return metadata_chunks_set, video_chunks_set

    def _search_for_match_chunks(self,
                                 lookup_paths: Iterator[str],
                                 mp4_chunks_left: set[str],
                                 match_chunk_extensions: list[str],
                                 bucket: str,
                                 artifact: ImageBasedArtifact) -> tuple[bool,
                                                                        set[str]]:
        """
        Search for metadata chunks on the provided paths and returns all metadata chunks found.
        The function will return as soon as all the matched chunks (compared to the video chunks) are found.

        Args:
            lookup_paths (Iterator[str]): Paths to search for on s3 bucket.
            mp4_chunks_left (set[str]): A set of the videos chunk ids (not the entire path) to be matched.
            match_chunk_extensions (list[str]): The chunks extensions to be search for.
            bucket (str): The S3 bucket to search on.
            messageid (str): The message id.

        Returns:
            tuple[bool, set[str]]: A tuple with a boolean that is true if all metadata is found and a set containing the path for all metadata.
        """

        metadata_chunks: set[str] = set()

        # Check metadata chunks
        for i, path in enumerate(lookup_paths):
            tmp_metadata_chunks, _ = self._search_chunks_in_s3_path(
                path, bucket, recorder_type=artifact.recorder.value, match_chunk_extensions=match_chunk_extensions)

            tmp_metadata_chunks_filtered = set()
            tmp_metadata_striped = set()

            # Ensure that only metadata belonging to the video are checked and return
            for chunk in tmp_metadata_chunks:
                mp4_key = self._apply_recording_regex(chunk)
                if mp4_key and mp4_key in mp4_chunks_left:
                    tmp_metadata_chunks_filtered.add(chunk)
                    tmp_metadata_striped.add(mp4_key)

            metadata_chunks = metadata_chunks.union(
                tmp_metadata_chunks_filtered)

            mp4_chunks_left = mp4_chunks_left - tmp_metadata_striped

            # If all metadata chunks were found returns
            if len(mp4_chunks_left) == 0:
                _logger.info(
                    "Metadata found outside of upload bounds, %d paths crawled", i)
                return True, metadata_chunks

        _logger.warning(
            "Fail to validate metadata for the following video chunks: %s", str(mp4_chunks_left))
        return False, metadata_chunks

    def _apply_recording_regex(self, chunk: str) -> Optional[str]:
        """
        Apply the recording regex to a chunk path.
        Args:
            chunk (str): The chunk path.

        Returns:
            str: The chunk id.
        """
        recording_regex = re.compile(r"/hour=\d{2}/(.+\.mp4).*")
        search_result = recording_regex.search(chunk)
        if not search_result:
            _logger.error("Failed to apply recording regex to chunk (%s)", chunk)
            return None
        return search_result.group(1)

    def _check_allparts_exist(self, artifact: MetadataArtifact) -> tuple[bool, set[str]]:
        """
        Checks if all metadata exists and are complete in RCC S3 between upload bounds (in the message).
        This function makes sure that we don't have a missing metacontent chunk for any video chunk.
        Athough, if there is a metacontent chunk which doesn't have a corresponding video chunk, it will get ingested eitherway.

        There are cases where this can be a huge problem, for example:
            If a metacontent chunk is missing and the video chunk could also not be found within the upload bounds
            This means that the function will still return True as if the metachunks were correctly ingested.

        There is no easy fix for this without using a prior message and storing a state.

        Args:
            artifacts (MetadataArtifact): The artifact coming from the queue.

        Returns:
            Tuple[
                bool,       :True if metadata is complete, False otherwise
                set[str]    :A set with all metadata chunks.
            ]
        """

        bucket = self._container_svcs.rcc_info["s3_bucket"]
        match_chunk_extension = self._get_file_extension()
        device_path = f'{artifact.referred_artifact.tenant_id}/{artifact.referred_artifact.device_id}/'

        s3_object_params = RCCS3ObjectParams(
            s3_path=device_path, bucket=bucket)

        # Make sure it has access to tenant and device
        if not self._container_svcs.check_if_tenant_and_deviceid_exists_and_log_on_error(
                self._rcc_s3_client, s3_object_params):
            return False, set()

        # Get all mp4 lookup paths
        mp4_lookup_paths = self._get_chunks_lookup_paths(artifact)

        if not mp4_lookup_paths:
            _logger.error(
                "No video chunks paths found for %s",
                f'{artifact.tenant_id}/{artifact.device_id}/')
            return False, set()

        _logger.debug("Searching for chunks")

        # Store all chunks in it's set
        mp4_chunks_left: set[str] = set()
        metadata_chunks: set[str] = set()

        # Search for video and metadata chunks
        for path in mp4_lookup_paths:
            tmp_metadata_chunks, tmp_mp4_chunks = self._search_chunks_in_s3_path(
                path, bucket,
                match_chunk_extensions=match_chunk_extension,
                recorder_type=artifact.referred_artifact.recorder.value,
                start_time=artifact.referred_artifact.upload_timing.start,
                end_time=artifact.referred_artifact.upload_timing.end)

            # Store only the recording name to ignore the folders before
            tmp_mp4_chunks_lst = [self._apply_recording_regex(chunk) for chunk in tmp_mp4_chunks]
            # remove the chunks where the regex failed
            tmp_mp4_chunks = set(filter(None, tmp_mp4_chunks_lst))

            metadata_chunks = metadata_chunks.union(tmp_metadata_chunks)
            mp4_chunks_left = mp4_chunks_left.union(tmp_mp4_chunks)

        tmp_metadata_filtered: set[str] = set()
        tmp_metadata_striped: set[str] = set()

        if len(mp4_chunks_left) == 0:
            _logger.error(
                "Could not find any video chunks for %s. Probably the chunks are out of upload bounds",
                f'{artifact.tenant_id}/{artifact.device_id}/')
            return False, set()

        # Ensure that only metadata belonging to the video are checked
        for chunk in metadata_chunks:

            # Strips the video from the metadata name
            mp4_key = self._apply_recording_regex(chunk)

            if mp4_key in mp4_chunks_left:
                tmp_metadata_filtered.add(chunk)
                tmp_metadata_striped.add(mp4_key)

        metadata_chunks = tmp_metadata_filtered

        # Remove video chunks where a metadata chunk was found
        mp4_chunks_left = mp4_chunks_left - tmp_metadata_striped

        if not mp4_chunks_left:
            _logger.info(
                "all metadata chunks found within upload bounds")
            return True, metadata_chunks

        _logger.debug("Not all metadata found within upload bounds, searching until %s", str(
            datetime.now()))

        # Search for the metadata paths until the current day
        metadata_paths = self._get_chunks_lookup_paths(
            artifact,
            start_time=artifact.referred_artifact.upload_timing.start,
            end_time=datetime.now())

        # Search for missing metadata chunks until the current day
        all_found, tmp_metadata_chunks = self._search_for_match_chunks(
            metadata_paths, mp4_chunks_left, match_chunk_extension, bucket, artifact.referred_artifact)

        metadata_chunks = metadata_chunks.union(tmp_metadata_chunks)

        _logger.info("Found %d metadata chunks", len(metadata_chunks))

        return all_found, metadata_chunks

    @abstractmethod
    def _get_file_extension(self) -> list[str]:
        """
        This function will be used to grab the file extension of the chunks to be searched for.

        Raises:
            NotImplementedError: Not implemented error.

        Returns:
            list[str]: A list containing the file extensions to search for.
        """
        raise NotImplementedError("The Metacontent file extension method needs to be overwritten")
