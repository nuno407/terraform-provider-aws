"""mongo controller service module"""
from typing import Union
import logging
from base.model.artifacts.api_messages import IMUDataArtifact, IMUSample
from base.model.artifacts import (CameraBlockedOperatorArtifact, CameraServiceEventArtifact, VideoSignalsData,
                                  DeviceInfoEventArtifact, IncidentEventArtifact, IMUArtifact,
                                  PeopleCountOperatorArtifact, S3VideoArtifact, SnapshotArtifact,
                                  SOSOperatorArtifact, PipelineProcessingStatus, AnonymizationResult)
from base.model.artifacts.upload_rule_model import SnapshotUploadRule, VideoUploadRule
from kink import inject
from artifact_api.models.mongo_models import DBSnapshotArtifact, SignalsSource
from artifact_api.mongo.services.mongo_event_service import MongoEventService
from artifact_api.mongo.services.mongo_imu_service import MongoIMUService
from artifact_api.mongo.services.mongo_sav_operator_service import MongoSavOperatorService
from artifact_api.mongo.services.mongo_recordings_service import MongoRecordingsService
from artifact_api.mongo.services.mongo_pipeline_service import MongoPipelineService
from artifact_api.mongo.services.mongo_signals_service import MongoSignalsService
from artifact_api.mongo.services.mongo_algorithm_output_service import MongoAlgorithmOutputService
from artifact_api.exceptions import IMUEmptyException


_logger = logging.getLogger(__name__)


@inject
class MongoService:  # pylint:disable=too-many-arguments
    """
    Mongo Controller Class
    """

    def __init__(
            self,
            mongo_event_controller: MongoEventService,
            mongo_imu_controller: MongoIMUService,
            sav_operator_feedback_controller: MongoSavOperatorService,
            mongo_recordings_controller: MongoRecordingsService,
            pipeline_processing_controller: MongoPipelineService,
            mongo_signals_controller: MongoSignalsService,
            mongo_algorithm_output_controller: MongoAlgorithmOutputService) -> None:

        self.__event_controller = mongo_event_controller
        self.__imu_controller = mongo_imu_controller
        self.__sav_operator_feedback_controller = sav_operator_feedback_controller
        self.__mongo_recordings_controller = mongo_recordings_controller
        self.__pipeline_processing_controller = pipeline_processing_controller
        self.__signals_controller = mongo_signals_controller
        self.__algorithm_output_controller = mongo_algorithm_output_controller

    async def update_videos_correlations(self, correlated_videos: list[str], snapshot_id: str):
        """_summary_
        """
        await self.__mongo_recordings_controller.update_videos_correlations(correlated_videos, snapshot_id)

    async def update_snapshots_correlations(self, correlated_snapshots: list[str], video_id: str):
        """_summary_
        """
        await self.__mongo_recordings_controller.update_snapshots_correlations(correlated_snapshots, video_id)

    async def upsert_snapshot(self, message: SnapshotArtifact, correlated_ids: list[str]):
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """
        await self.__mongo_recordings_controller.upsert_snapshot(message, correlated_ids)

    async def upsert_video(self, message: S3VideoArtifact, correlated_ids: list[str]):
        """_summary_
        Args:
            message (S3VideoArtifact): _description_
        """
        await self.__mongo_recordings_controller.upsert_video(message, correlated_ids)

    async def get_correlated_videos_for_snapshot(self, message: SnapshotArtifact) -> list[DBSnapshotArtifact]:
        """_summary_

        Args:
            message (SnapshotArtifact): _description_
        """

        return await self.__mongo_recordings_controller.get_correlated_videos_for_snapshot(message)

    async def get_correlated_snapshots_for_video(self, message: S3VideoArtifact) -> list[DBSnapshotArtifact]:

        """_summary_

        Args:
            message (S3VideoArtifact): _description_
        """

        return await self.__mongo_recordings_controller.get_correlated_snapshots_for_video(message)

    async def create_event(self, message: Union[CameraServiceEventArtifact,
                                                DeviceInfoEventArtifact,
                                                IncidentEventArtifact]):
        """
        Creates DB Event Artifact and writes the document to the mongoDB
        Args:
            message (Union[CameraServiceEventArtifact,
                            DeviceInfoEventArtifact,
                            IncidentEventArtifact]): Event Artifact
        Raises:
            UnknowEventArtifactException:

        Returns:
            (DBCameraServiceEventArtifact,
                DBDeviceInfoEventArtifact,
                DBIncidentEventArtifact): Corresponding DB Event Artifact
        """
        await self.__event_controller.save_event(message)

    # Operator Events
    async def create_operator_feedback_event(self, artifact: Union[SOSOperatorArtifact,
                                                                   PeopleCountOperatorArtifact,
                                                                   CameraBlockedOperatorArtifact]):
        """
        Create operator feedback entry in database
        Args:
            artifact: The artifact to store
        """

        await self.__sav_operator_feedback_controller.save_event(artifact)

    async def load_device_video_signals_data(self, device_video_signals: VideoSignalsData):
        """_summary_

        Args:
            device_video_signals (VideoSignalsData): _description_
        """
        _logger.debug("Inserting video signals to : %s",
                      device_video_signals.video_raw_s3_path)
        await self.__signals_controller.save_signals(device_video_signals.data, SignalsSource.MDF_PARSER, device_video_signals.correlation_id)
        _logger.debug("Inserting aggregated_metadata : %s",
                      str(device_video_signals.aggregated_metadata))
        await self.__mongo_recordings_controller.upsert_video_aggregated_metadata(device_video_signals.aggregated_metadata, device_video_signals.correlation_id)
        _logger.info(
            "Video signals have been processed successfully to mongoDB")

    async def process_imu_artifact(self, imu_data_artifact: IMUDataArtifact):
        """_summary_

        Args:
            imu_data_artifact (IMUDataArtifact): _description_
        """

        imu_data: list[IMUSample] = imu_data_artifact.data.root
        imu_artifact: IMUArtifact = imu_data_artifact.message
        imu_tenant = imu_artifact.tenant_id
        imu_device = imu_data[0].source.device_id

        if len(imu_data) == 0:
            _logger.warning(
                "The imu sample list does not contain any information")
            raise IMUEmptyException()

        _logger.debug("Inserting IMU data from artifact: %s",
                      str(imu_artifact))

        imu_ranges = await self.__imu_controller.insert_imu_data(imu_data)

        for imu_range in imu_ranges:
            await self.__event_controller.update_events(imu_range, imu_tenant, imu_device, "imu")

    async def attach_rule_to_video(self, message: VideoUploadRule) -> None:
        """ Attaches the upload rule to the given video. """
        await self.__mongo_recordings_controller.attach_rule_to_video(message)

    async def attach_rule_to_snapshot(self, message: SnapshotUploadRule) -> None:
        """ Attaches the upload rule to the given snapshot. """
        await self.__mongo_recordings_controller.attach_rule_to_snapshot(message)

    async def create_pipeline_processing_status(self, message: PipelineProcessingStatus, last_updated: str):
        """Creates DB Pipeline Processing Status Artifact and writes the document to the mongoDB

        Args:
            message (PipelineProcessingStatus): Pipeline Processing Status Artifact
            last_updated (str): Last Updated date as a string

        Returns:
            DBPipelineProcessingStatus: Corresponding DB Pipeline Processing Status Artifact
        """
        await self.__pipeline_processing_controller.save_pipeline_processing_status(message, last_updated)

    async def create_anonymization_result_output(self, message: AnonymizationResult, last_updated: str):
        """Saves the algorithm output to the database

        Args:
            message (Union[SignalsFrame, IMUDataArtifact]): Algorithm output
        """
        await self.__algorithm_output_controller.save_anonymization_result_output(message)
        _logger.info(
            "Algorithm output has been processed successfully to mongoDB")

        await self.__pipeline_processing_controller.update_pipeline_processing_status_anonymization(message, last_updated)
        _logger.info(
            "Pipeline processing status has been updated successfully to mongoDB")
