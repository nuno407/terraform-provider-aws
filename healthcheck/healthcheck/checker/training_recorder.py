# pylint: disable=too-few-public-methods
"""Training recorder checker module."""
import logging

from kink import inject

from healthcheck.checker.artifact import BaseArtifactChecker
from healthcheck.model import Artifact
from healthcheck.voxel_client import VoxelDataset

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=BaseArtifactChecker)
class TrainingRecorderArtifactChecker(BaseArtifactChecker):
    """Training recorder artifact checker."""

    def run_healthcheck(self, artifact: Artifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        _logger.info("running healthcheck for training recorder")
        # Check if data ingestion is marked as complete
        self._is_data_status_complete_or_raise(artifact)

        video_id = artifact.artifact_id
        # Check s3 files
        self._is_s3_raw_file_presence_or_raise(f"{video_id}.mp4", artifact)

        # This checks can be improved by checking dinamically based on algorithm output
        self._is_s3_anonymized_file_present_or_raise(f"{video_id}_anonymized.mp4", artifact)
        self._is_s3_anonymized_file_present_or_raise(f"{video_id}_chc.json", artifact)

        # Check DB
        self._is_recordings_doc_valid_or_raise(artifact)
        self._is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(artifact)

        # Perform Voxel validations
        self._is_fiftyone_entry_present_or_raise(artifact, VoxelDataset.VIDEOS)
