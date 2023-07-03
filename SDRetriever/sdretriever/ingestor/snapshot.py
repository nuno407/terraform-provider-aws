""" snapshot module """
import logging as log
from datetime import datetime
from typing import TypeVar

from kink import inject

from base.aws.container_services import ContainerServices
from base.aws.s3 import S3ClientFactory, S3Controller
from base.model.artifacts import Artifact, SnapshotArtifact
from sdretriever.config import SDRetrieverConfig
from sdretriever.constants import FileExt
from sdretriever.exceptions import FileAlreadyExists
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3_finder import S3Finder

_logger = log.getLogger("SDRetriever." + __name__)

ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type


@inject
class SnapshotIngestor(Ingestor):
    """ Snapshot ingestor """

    def __init__(
            self,
            container_services: ContainerServices,
            rcc_s3_client_factory: S3ClientFactory,
            s3_controller: S3Controller,
            config: SDRetrieverConfig,
            s3_finder: S3Finder) -> None:
        super().__init__(container_services, rcc_s3_client_factory, s3_finder, s3_controller)
        self._s3_controller = s3_controller
        self._config = config

    def ingest(self, artifact: Artifact) -> None:
        """ Ingests a snapshot artifact """
        # validate that we are parsing a SnapshotArtifact
        if not isinstance(artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngestor can only ingest a SnapshotArtifact")

        # Initialize file name and path
        snap_name = f"{artifact.artifact_id}{FileExt.SNAPSHOT.value}"
        snap_path = f"{artifact.tenant_id}/{snap_name}"

        # Checks if exists in devcloud
        exists_on_devcloud = self._s3_controller.check_s3_file_exists(
            self._container_svcs.raw_s3, snap_path)

        if exists_on_devcloud and self._config.discard_video_already_ingested:
            message = f"File {snap_path} already exists on {self._container_svcs.raw_s3}"
            _logger.info(message)
            raise FileAlreadyExists(message)

        rcc_s3_bucket = self._container_svcs.rcc_info.get('s3_bucket')
        # download the file from RCC - an exception is raised if the file is not found
        jpeg_data = self.get_file_in_rcc(rcc_s3_bucket,
                                         artifact.tenant_id,
                                         artifact.device_id,
                                         artifact.uuid,
                                         artifact.timestamp,
                                         datetime.now(),
                                         [".jpeg",
                                          ".png"])

        # Upload files to DevCloud
        snap_full_path = self._upload_file(snap_path, jpeg_data)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", snap_full_path)
        artifact.s3_path = snap_full_path
