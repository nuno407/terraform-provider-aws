"""metacontent module"""
import json
import pytz
import logging as log
from kink import inject
from datetime import datetime

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, SignalsArtifact
from sdretriever.constants import FileExt
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.s3.s3_chunk_downloader_rcc import RCCChunkDownloader
from sdretriever.models import ChunkDownloadParamsByID, S3ObjectDevcloud, S3ObjectRCC
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3.s3_downloader_uploader import S3DownloaderUploader

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class VideoMetadataIngestor(Ingestor):  # pylint: disable=too-few-public-methods
    """ Video metadata ingestor """

    def __init__(self,
                 s3_chunk_ingestor: RCCChunkDownloader,
                 metadata_merger: MetadataMerger,
                 s3_interface: S3DownloaderUploader):
        self.__metadata_merger = metadata_merger
        self.__s3_chunk_ingestor = s3_chunk_ingestor
        self.__s3_interface = s3_interface

    def __download_all_chunks(self, artifact: SignalsArtifact) -> list[S3ObjectRCC]:
        """
        Download all chunks from RCC

        Args:
            artifact (SignalsArtifact): The signals artifact

        Returns:
            list[S3ObjectRCC]: All the chunks downloaded
        """

        downloaded_chunks : list[S3ObjectRCC] = []

        for recording in artifact.referred_artifact.recordings:
            params = ChunkDownloadParamsByID(
                recorder=artifact.referred_artifact.recorder,
                recording_id=recording.recording_id,
                chunk_ids=recording.chunk_ids,
                device_id=artifact.device_id,
                tenant=artifact.tenant_id,
                start_search=artifact.referred_artifact.timestamp,
                stop_search=datetime.now(tz=pytz.UTC),
                suffixes=[".json.zip"])

            downloaded_chunks.extend(self.__s3_chunk_ingestor.download_by_chunk_id(params))

        return downloaded_chunks



    def __upload_metadata(self, source_data: dict, artifact: Artifact) -> str:
        """Store source data on our raw_s3 bucket

        Args:
            source_data (dict): data to be stored
            message (Artifact): Message object

        Raises:
            exception: If an error has ocurred while uploading.

        Returns:
            s3_upload_path (str): Path where file got stored. Returns None if upload fails.
        """
        source_data_as_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False).encode('UTF-8'))
        filename = artifact.artifact_id + FileExt.METADATA.value

        devcloud_object = S3ObjectDevcloud(
            data=source_data_as_bytes,
            filename=filename,
            tenant=artifact.tenant_id)
        return self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

    def ingest(self, artifact: Artifact) -> None:
        # validate that we are parsing a SignalsArtifact
        if not isinstance(artifact, SignalsArtifact):
            raise ValueError("SignalsIngestor can only ingest a SignalsArtifact")

        downloaded_chunks = self.__download_all_chunks(artifact)
        mdf_chunks = self.__metadata_merger.merge_metadata_chunks(downloaded_chunks)
        mdf_s3_path = self.__upload_metadata(mdf_chunks, artifact)

        # Store the MDF path in the artifact
        artifact.s3_path = mdf_s3_path
