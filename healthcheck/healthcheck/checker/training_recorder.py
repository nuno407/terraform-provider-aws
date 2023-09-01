# type: ignore
# pylint: disable=too-few-public-methods, duplicate-code, line-too-long
"""Training recorder checker module."""
import logging

from kink import inject

from base.model.artifacts import VideoArtifact
from healthcheck.config import HealthcheckConfig
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.s3_utils import S3Utils

_logger: logging.Logger = logging.getLogger(__name__)


@inject()
class TrainingRecorderArtifactChecker:
    """Training recorder artifact checker."""

    def __init__(
            self,
            config: HealthcheckConfig,
            s3_utils: S3Utils,
            db_controller: DatabaseController,
            voxel_fiftyone_controller: VoxelFiftyOneController):
        self.__config = config
        self.__s3_utils = s3_utils
        self.__db_controller = db_controller
        self.__voxel_fiftyone_controller = voxel_fiftyone_controller

    def __is_relevant(self, artifact: VideoArtifact) -> bool:
        return artifact in self.__config.training_whitelist

    def run_healthcheck(self, artifact: VideoArtifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        # skip artifact if not relevant
        if not self.__is_relevant(artifact):
            _logger.info("skipping %s, artifact is not training recorder whitelisted", artifact.artifact_id)
            return

        _logger.info("running healthcheck for training recorder")
        # Check if data ingestion is marked as complete
        self.__db_controller.is_data_status_complete_or_raise(artifact)

        video_id = artifact.artifact_id
        # Check s3 files
        self.__s3_utils.is_s3_raw_file_present_or_raise(f"{video_id}.mp4", artifact)

        # This checks can be improved by checking dinamically based on algorithm output
        self.__s3_utils.is_s3_anonymized_file_present_or_raise(f"{video_id}_anonymized.mp4", artifact)
        self.__s3_utils.is_s3_anonymized_file_present_or_raise(f"{video_id}_chc.json", artifact)

        # Check DB
        self.__db_controller.is_recordings_doc_valid_or_raise(artifact)
        self.__db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(
            artifact)

        # Perform Voxel validations
        self.__voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise(artifact)
