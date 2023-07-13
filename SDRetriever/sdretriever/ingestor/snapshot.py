""" snapshot module """
import logging as log
from datetime import datetime
from typing import TypeVar

from kink import inject
import pytz

from base.model.artifacts import Artifact, SnapshotArtifact
from sdretriever.constants import FileExt
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.models import S3ObjectDevcloud, RCCS3SearchParams
from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.s3_downloader_uploader import S3DownloaderUploader

_logger = log.getLogger("SDRetriever." + __name__)

ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type


@inject
class SnapshotIngestor(Ingestor):
    """ Snapshot ingestor """

    def __init__(
            self,
            s3_interface: S3DownloaderUploader) -> None:
        self.__s3_interface = s3_interface

    def ingest(self, artifact: Artifact) -> None:
        """ Ingests a snapshot artifact """
        # validate that we are parsing a SnapshotArtifact
        if not isinstance(artifact, SnapshotArtifact):
            raise ValueError("SnapshotIngestor can only ingest a SnapshotArtifact")

        # Initialize file name and path
        snap_name = f"{artifact.artifact_id}{FileExt.SNAPSHOT.value}"

        # Download data
        params = RCCS3SearchParams(
            device_id=artifact.device_id,
            tenant=artifact.tenant_id,
            start_search=artifact.timestamp,
            stop_search=datetime.now(
                tz=pytz.UTC))

        downloaded_object = self.__s3_interface.search_and_download_from_rcc(
            file_name=artifact.uuid, search_params=params)

        # Upload files to DevCloud
        devcloud_object = S3ObjectDevcloud(
            data=downloaded_object.data,
            filename=snap_name,
            tenant=artifact.tenant_id)

        path_uploaded = self.__s3_interface.upload_to_devcloud_raw(devcloud_object)

        # update artifact with s3 path
        _logger.info("Successfully uploaded to %s", path_uploaded)
        artifact.s3_path = path_uploaded
