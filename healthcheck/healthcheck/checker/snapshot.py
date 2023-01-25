# type: ignore
# pylint: disable=too-few-public-methods
"""Snapshot artifact module."""
import logging

from kink import inject

from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController

from healthcheck.checker.common import ArtifactChecker
from healthcheck.exceptions import FailDocumentValidation, NotYetIngestedError
from healthcheck.model import Artifact
from healthcheck.voxel_client import VoxelDataset

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=ArtifactChecker)
class SnapshotArtifactChecker:
    """Snapshot artifact checker."""

    def __init__(
            self,
            s3_controller: S3Controller,
            db_controller: DatabaseController,
            voxel_fiftyone_controller: VoxelFiftyOneController):
        self.__s3_controller = s3_controller
        self.__db_controller = db_controller
        self.__voxel_fiftyone_controller = voxel_fiftyone_controller

    def run_healthcheck(self, artifact: Artifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        _logger.info("running healthcheck for snapshot")
        # Check if data ingestion is marked as complete
        self.__db_controller.is_data_status_complete_or_raise(artifact)

        snapshot_id = artifact.artifact_id
        # Check S3 files
        self.__s3_controller.is_s3_raw_file_present_or_raise(
            f"{snapshot_id}.jpeg", artifact)
        self.__s3_controller.is_s3_anonymized_file_present_or_raise(
            f"{snapshot_id}_anonymized.jpeg", artifact)

        # Check database if recording metadata is present and according to jsonschema
        try:
            self.__db_controller.is_recordings_doc_valid_or_raise(artifact)
        except FailDocumentValidation as err:
            if err.json_path == "$.recording_overview.source_videos":
                raise NotYetIngestedError(
                    err.artifact,
                    "No recording matching the snapshot has been ingested yet.") from err
        # Check if voxel 51 entry is present
        self.__voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise(
            artifact, VoxelDataset.SNAPSHOTS)
