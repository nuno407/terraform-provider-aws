# type: ignore
# pylint: disable=too-few-public-methods, line-too-long
"""Interior recorder checker module."""
import logging

from kink import inject

from base.model.artifacts import VideoArtifact
from healthcheck.checker.common import ArtifactChecker
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.s3_utils import S3Utils

_logger: logging.Logger = logging.getLogger(__name__)


@inject(alias=ArtifactChecker)
class InteriorRecorderArtifactChecker:
    """Interior recorder artifact checker."""

    def __init__(
            self,
            s3_controller: S3Utils,
            db_controller: DatabaseController,
            voxel_fiftyone_controller: VoxelFiftyOneController):
        self.__s3_utils = s3_controller
        self.__db_controller = db_controller
        self.__voxel_fiftyone_controller = voxel_fiftyone_controller

    def run_healthcheck(self, artifact: VideoArtifact) -> None:
        """
        Run healthcheck

        Args:
            artifact (Artifact): data ingestion artifact
        """
        _logger.info("running healthcheck for interior recorder")
        # check if data ingestion is marked as complete
        self.__db_controller.is_data_status_complete_or_raise(artifact)

        video_id = artifact.artifact_id
        # Check s3 files
        self.__s3_utils.is_s3_raw_file_present_or_raise(f"{video_id}.mp4", artifact)
        self.__s3_utils.is_s3_raw_file_present_or_raise(f"{video_id}_signals.json", artifact)
        self.__s3_utils.is_s3_raw_file_present_or_raise(f"{video_id}_metadata_full.json", artifact)

        # This checks can be improved by checking dinamically based on algorithm output
        self.__s3_utils.is_s3_anonymized_file_present_or_raise(f"{video_id}_anonymized.mp4", artifact)
        self.__s3_utils.is_s3_anonymized_file_present_or_raise(f"{video_id}_chc.json", artifact)

        # Check DB
        self.__db_controller.is_recordings_doc_valid_or_raise(artifact)
        self.__db_controller.is_signals_doc_valid_or_raise(artifact)
        self.__db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(artifact)

        # Perform Voxel validations
        self.__voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise(artifact)
