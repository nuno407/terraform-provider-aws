""" imu module. """
import logging as log
import re
import sys
from typing import Optional

from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.message.video import VideoMessage

IMU_FILE_EXT = '_imu.csv'
LOGGER = log.getLogger("SDRetriever." + __name__)
IMU_FILE_PATTERN = r"TrainingRecorder_TrainingRecorder.+\.mp4\._\w+?_\w+?_(\d+)_imu_raw.csv.zip"


class IMUIngestor(MetacontentIngestor):
    """_summary_

    Args:
        MetacontentIngestor (_type_): _description_
    """

    def __init__(self, container_services, s3_client, sqs_client, sts_helper) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)
        self.imu_regex = re.compile(IMU_FILE_PATTERN)

    def __get_id_from_imu(self, imu_chunk: MetacontentChunk) -> int:
        """
        Parses the ID of an IMU chunk path.

        Args:
            imu_chunk (MetacontentChunk): The IMU in RCC

        Raises:
            ValueError: When the IMU path does not match a valid path.

        Returns:
            int: The ID of the chunk.
        """
        match = self.imu_regex.search(imu_chunk.filename)
        if match is None:
            raise ValueError(f"The following imu path {imu_chunk.filename} does \
                             not fit the pattern of an imu path")
        return int(match.group(1))

    @staticmethod
    def concatenate_chunks(chunks: list[MetacontentChunk]) -> bytearray:
        """
        Concatenate the IMU chunks into a single one.
        Since the imu chunks can by large, to avoid future performance bootlenecks:
        1 - It will calculate the size of the chunks
        2 - A buffer will be pre-allocated
        3 - Each chunk will be written into this buffer

        This avoids multiple allocations, preventing memory issues and speeding up the process.

        Args:
            chunks (list[MetacontentChunk]): List of chunks to be merged
            video_id (str): The video ID

        Returns:
            bytearray: The file ready to be uploaded
        """
        size_data = 0

        # Measure size of actual data
        for chunk in chunks:
            size_data += sys.getsizeof(chunk.data)

        # Allocates a continuous chunk of memory
        data_buffer = bytearray(size_data)

        # Write to the buffer
        buffer_counter = 0
        for chunk in chunks:
            data_buffer[buffer_counter:] = chunk.data
            buffer_counter += sys.getsizeof(chunk.data)

        return data_buffer

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [".csv.zip"]

    def ingest(self, video_msg: VideoMessage, video_id: str,
               imu_chunk_paths: set[str]) -> Optional[str]:
        """
        Concatenates the IMU chunks and ingest them.

        Args:
            video_msg (VideoMessage): The video messsage
            video_id (str): The video ID
            metadata_chunk_paths (set[str]): A set containing the
            paths or keys to find the imu chunks

        Returns:
            _type_: _description_
        """
        # Fetch metadata chunks from RCC S3
        chunks = self._get_metacontent_chunks(video_msg, imu_chunk_paths)

        if not chunks:
            return None

        # Sorts and validates the chunks
        chunks.sort(key=self.__get_id_from_imu)

        LOGGER.debug("Ingesting %d IMU chunks", len(chunks))

        # Concatenate chunks and force deletion
        file_binary: bytearray = IMUIngestor.concatenate_chunks(chunks)
        del chunks
        file_to_upload = MetacontentDevCloud(file_binary, video_id, self.container_svcs.raw_s3,
                                             str(video_msg.tenant) + "/", IMU_FILE_EXT)

        # Upload IMU chunks and force deletion
        path_uploaded = self._upload_metacontent_to_devcloud(file_to_upload)
        del file_to_upload

        return path_uploaded
