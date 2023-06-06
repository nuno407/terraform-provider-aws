""" imu module. """
import logging as log
import re
import sys

from kink import inject
from sdretriever.exceptions import UploadNotYetCompletedError
from sdretriever.ingestor.metacontent import (MetacontentChunk,
                                              MetacontentDevCloud,
                                              MetacontentIngestor)
from sdretriever.s3_finder_rcc import S3FinderRCC
from sdretriever.constants import VIDEO_CHUNK_REGX
from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, IMUArtifact

IMU_FILE_EXT = '.csv'
_logger = log.getLogger("SDRetriever." + __name__)
IMU_FILE_PATTERN = r"TrainingRecorder_TrainingRecorder.+\.mp4\._\w+?_\w+?_(\d+)_imu_raw.csv.zip"
IMU_REGEX = re.compile(IMU_FILE_PATTERN)


@inject
class IMUIngestor(MetacontentIngestor):  # pylint: disable=too-few-public-methods
    """ IMU ingestor """

    def __init__(self,
                 container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 s3_controller: S3Controller,
                 s3_finder: S3FinderRCC):
        super().__init__(container_services, rcc_s3_client_factory,
                         s3_controller, s3_finder, VIDEO_CHUNK_REGX)

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
        match = IMU_REGEX.search(imu_chunk.s3_key)
        if match is None:
            raise ValueError(f"The following imu path {imu_chunk.s3_key} does \
                             not fit the pattern of an imu path")
        return int(match.group(1))

    def _concatenate_chunks(self, chunks: list[MetacontentChunk]) -> bytearray:
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

    def ingest(self, artifact: Artifact) -> None:
        """
        Concatenates the IMU chunks and ingest them.

        Args:
            artifact (IMUArtifact): The IMU artifact to be ingested
        """
        # validate that we are parsing an IMUArtifact
        if not isinstance(artifact, IMUArtifact):
            raise ValueError("IMUIngestor can only ingest an IMUArtifact")

        is_complete, chunk_paths = self._check_allparts_exist(artifact)
        if not is_complete:
            message = "IMU data is not complete, skipping for now"
            _logger.info(message)
            raise UploadNotYetCompletedError(message)

        # Fetch metadata chunks from RCC S3
        chunks: list[MetacontentChunk] = self._get_metacontent_chunks(
            chunk_paths)
        if not chunks:
            _logger.warning(
                "Could not find any metadata files for %s",
                artifact.artifact_id)
        # Sorts and validates the chunks
        chunks.sort(key=self.__get_id_from_imu)

        _logger.debug("Ingesting %d IMU chunks", len(chunks))

        # Concatenate chunks and force deletion
        file_binary: bytearray = self._concatenate_chunks(chunks)
        del chunks

        s3_bucket = self._container_svcs.raw_s3
        msp = artifact.tenant_id
        file_to_upload = MetacontentDevCloud(
            file_binary, artifact.artifact_id, s3_bucket,
            msp, IMU_FILE_EXT)

        # Upload IMU chunks and force deletion
        path_uploaded = self._upload_metacontent_to_devcloud(file_to_upload)
        del file_to_upload

        # Update the artifact with the path to the IMU file
        artifact.s3_path = path_uploaded
