# type: ignore
# pylint: disable=too-few-public-methods
"""Artifact checker module."""
from abc import ABC, abstractmethod

from kink import inject

from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.model import Artifact
from healthcheck.voxel_client import VoxelDataset


@inject
class BaseArtifactChecker(ABC):
    """This class acts as an interface and shall have common functions
    between the artifact types to decrease code duplication"""

    def __init__(
            self,
            blob_controller: S3Controller,
            db_controller: DatabaseController,
            voxel_fiftyone_controller: VoxelFiftyOneController):
        self.__blob_controller = blob_controller
        self.__db_controller = db_controller
        self.__voxel_fiftyone_controller = voxel_fiftyone_controller

    def _is_s3_anonymized_file_present_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """Calls blob controller to check if anon file is present in S3, raises exception if not."""
        self.__blob_controller.is_s3_anonymized_file_present_or_raise(
            file_name, artifact)

    def _is_s3_raw_file_presence_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """Calls blob controller to check if raw file is present in S3, raises exception if not."""
        self.__blob_controller.is_s3_raw_file_presence_or_raise(
            file_name, artifact)

    def _is_signals_doc_valid_or_raise(self, artifact: Artifact) -> list:
        """Calls db controller to check if signals documents are valid."""
        return self.__db_controller.is_signals_doc_valid_or_raise(artifact)

    def _is_recordings_doc_valid_or_raise(self, artifact: Artifact) -> dict:
        """Calls db controller to check if recordings documents are valid."""
        return self.__db_controller.is_recordings_doc_valid_or_raise(artifact)

    def _is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(self, artifact: Artifact) -> list:  # pylint: disable=line-too-long
        """Calls db controller to check if pipeline execution and algo output documents are valid"""
        return self.__db_controller.is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(
            artifact)

    def _is_data_status_complete_or_raise(self, artifact: Artifact) -> None:
        """Calls db controller to check if data_status is marked as completed for given artifact."""
        return self.__db_controller.is_data_status_complete_or_raise(artifact)

    def _is_fiftyone_entry_present_or_raise(self, artifact: Artifact, dataset: VoxelDataset) -> None:  # pylint: disable=line-too-long
        """Calls voxel51 controller to check if artifact entry is present in given dataset."""
        return self.__voxel_fiftyone_controller.is_fiftyone_entry_present_or_raise(
            artifact, dataset)

    @abstractmethod
    def run_healthcheck(self, artifact: Artifact) -> None:
        """Runs the healthcheck for the given artifact"""
