# type: ignore
# pylint: disable=too-few-public-methods, duplicate-code
"""Training recorder checker module."""
import logging

from kink import inject

from base.aws.s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController


from healthcheck.checker.common import ArtifactChecker
from healthcheck.model import Artifact

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=ArtifactChecker)
class TrainingRecorderArtifactChecker:
    """Training recorder artifact checker."""

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
        _logger.info("running healthcheck for training recorder")
        # Check if data ingestion is marked as complete
        self.__db_controller.is_data_status_complete_or_raise(artifact)

        video_id = artifact.artifact_id
        # Check s3 files
        self.__s3_controller.is_s3_raw_file_present_or_raise(f"{video_id}.mp4", artifact)

        # This checks can be improved by checking dinamically based on algorithm output
        self.__s3_controller.is_s3_anonymized_file_present_or_raise(
            f"{video_id}_anonymized.mp4", artifact)
        self.__s3_controller.is_s3_anonymized_file_present_or_raise(
            f"{video_id}_chc.json", artifact)

        # Check DB
        self.__db_controller.is_recordings_doc_valid_or_raise(artifact)
        self.__db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(
            artifact)

        # Perform Voxel validations
        self.__voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise(artifact)
