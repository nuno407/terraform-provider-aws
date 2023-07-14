""" imu module. """
import logging as log
import re
import sys
import pytz

from datetime import datetime
from kink import inject

from base.model.artifacts import Artifact, IMUArtifact
from sdretriever.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.models import ChunkDownloadParams, S3ObjectDevcloud, S3ObjectRCC
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3_downloader_uploader import S3DownloaderUploader

IMU_FILE_EXT = '.csv'
_logger = log.getLogger("SDRetriever." + __name__)
IMU_FILE_PATTERN = r"TrainingRecorder_TrainingRecorder.+\.mp4\._\w+?_\w+?_(\d+)_imu_raw.csv.zip"
IMU_REGEX = re.compile(IMU_FILE_PATTERN)


@inject
class IMUIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """ IMU ingestor """

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 s3_interface: S3DownloaderUploader):
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__s3_interface = s3_interface

    def __concatenate_chunks(self, chunks: list[S3ObjectRCC]) -> bytearray:
        """
        Concatenate the IMU chunks into a single one.
        Since the imu chunks can by large, to avoid future performance bootlenecks:
        1 - It will calculate the size of the chunks
        2 - A buffer will be pre-allocated
        3 - Each chunk will be written into this buffer

        This avoids multiple allocations, preventing memory issues and speeding up the process.

        Args:
            chunks (list[S3ObjectRCC]): List of chunks to be merged
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

    def ingest(self, artifact: Artifact) -> None:
        """
        Concatenates the IMU chunks and ingest them.

        Args:
            artifact (IMUArtifact): The IMU artifact to be ingested
        """
        # validate that we are parsing an IMUArtifact
        if not isinstance(artifact, IMUArtifact):
            raise ValueError("IMUIngestor can only ingest an IMUArtifact")

        params = ChunkDownloadParams(
            recorder=artifact.referred_artifact.recorder,
            recording_id=artifact.referred_artifact.recording_id,
            chunk_ids=artifact.referred_artifact.chunk_ids,
            device_id=artifact.device_id,
            tenant=artifact.tenant_id,
            start_search=artifact.referred_artifact.timestamp,
            stop_search=datetime.now(tz=pytz.UTC),
            suffix="_imu_raw.csv.zip")

        downloaded_chunks = self.__s3_chunk_ingestor.download_files(params)

        # Concatenate chunks and force deletion
        file_binary: bytearray = self.__concatenate_chunks(downloaded_chunks)
        del downloaded_chunks

        devcloud_object = S3ObjectDevcloud(
            data=file_binary,
            filename=f"{artifact.artifact_id}/{IMU_FILE_EXT}",
            tenant=artifact.tenant_id)

        # Upload IMU chunks and force deletion
        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)
        del devcloud_object

        # Update the artifact with the path to the IMU file
        artifact.s3_path = path_uploaded
