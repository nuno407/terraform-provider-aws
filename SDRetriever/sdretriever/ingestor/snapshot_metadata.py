"""metacontent module"""
import logging as log
from datetime import datetime

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, MetadataArtifact, SnapshotArtifact
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import SNAPSHOT_CHUNK_REGX, FileExt
from sdretriever.exceptions import FileAlreadyExists
from sdretriever.ingestor.metacontent import MetacontentIngestor
from sdretriever.s3_finder_rcc import S3FinderRCC

_logger = log.getLogger("SDRetriever." + __name__)


@inject
class SnapshotMetadataIngestor(MetacontentIngestor):  # pylint: disable=too-few-public-methods
    """ snapshot's metadata ingestor """

    def __init__(self,
                 container_services: ContainerServices,
                 rcc_s3_client_factory: S3ClientFactory,
                 s3_controller: S3Controller,
                 config: SDRetrieverConfig,
                 s3_finder: S3FinderRCC):
        super().__init__(container_services, rcc_s3_client_factory,
                         s3_controller, s3_finder, SNAPSHOT_CHUNK_REGX)
        self.__config = config

    def _get_file_extension(self) -> list[str]:
        """
        Return the file extension of the metadata

        Returns:
            list[str]: _description_
        """
        return [FileExt.METADATA.value, FileExt.ZIPPED_METADATA.value]

    def ingest(self, artifact: Artifact) -> None:
        """ Ingests a snapshot artifact """
        # validate that we are parsing a SnapshotArtifact
        if not isinstance(artifact, MetadataArtifact):
            raise ValueError("SnapshotIngestor can only ingest a MetadataArtifact")
        if not isinstance(artifact.referred_artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngeArtifact can only ingest snapshot related metadata")

        # Initialize file name and path
        metadata_snap_name = f"{artifact.artifact_id}{FileExt.METADATA.value}"
        metadata_snap_path = f"{artifact.tenant_id}/{metadata_snap_name}"

        # Checks if exists in devcloud
        exists_on_devcloud = self._s3_controller.check_s3_file_exists(
            self._container_svcs.raw_s3, metadata_snap_path)

        if exists_on_devcloud and self.__config.discard_video_already_ingested:
            message = f"File {metadata_snap_path} already exists on {self._container_svcs.raw_s3}"
            _logger.info(message)
            raise FileAlreadyExists(message)

        rcc_s3_bucket = self._container_svcs.rcc_info.get('s3_bucket')
        # download the file from RCC - an exception is raised if the file is not found
        jpeg_metadata = self.get_file_in_rcc(rcc_s3_bucket,
                                             artifact.tenant_id,
                                             artifact.device_id,
                                             artifact.referred_artifact.uuid,
                                             artifact.referred_artifact.timestamp,
                                             datetime.now(),
                                             self._get_file_extension())

        # Upload files to DevCloud
        snap_full_path = self._upload_file(metadata_snap_path, jpeg_metadata)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", snap_full_path)
        artifact.s3_path = snap_full_path
