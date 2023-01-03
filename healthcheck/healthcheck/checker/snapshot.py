"""Snapshot artifact module."""
import logging

from kink import inject

from healthcheck.checker.artifact import BaseArtifactChecker
from healthcheck.model import Artifact
from healthcheck.voxel_client import VoxelDataset
from healthcheck.exceptions import FailDocumentValidation, NotYetIngestedError

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=BaseArtifactChecker)
class SnapshotArtifactChecker(BaseArtifactChecker):
    """Snapshot artifact checker."""

    def run_healthcheck(self, artifact: Artifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        _logger.info("running healthcheck for snapshot")
        # Check if data ingestion is marked as complete
        self._is_data_status_complete_or_raise(artifact)

        snapshot_id = artifact.artifact_id
        # Check S3 files
        self._is_s3_raw_file_presence_or_raise(f"{snapshot_id}.jpeg", artifact)
        self._is_s3_anonymized_file_present_or_raise(f"{snapshot_id}_anonymized.jpeg", artifact)

        # Check database if recording metadata is present and according to jsonschema
        try:
            self._is_recordings_doc_valid_or_raise(artifact)
        except FailDocumentValidation as e:
            if e.json_path == "$.recording_overview.source_videos":
                raise NotYetIngestedError(e.artifact, "No recording matching the snapshot has been ingested yet.")
            raise e

        # Check if voxel 51 entry is present
        self._is_fiftyone_entry_present_or_raise(artifact, VoxelDataset.SNAPSHOTS)
