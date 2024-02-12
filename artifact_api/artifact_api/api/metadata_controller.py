"""Router for metadata"""
import logging
from kink import di
from fastapi import APIRouter, Depends
from fastapi_restful.cbv import cbv
from base.model.artifacts.api_messages import IMUDataArtifact, VideoSignalsData, SnapshotSignalsData
from artifact_api.models import ResponseMessage
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.voxel.service import VoxelService

_logger = logging.getLogger(__name__)
metadata_router = APIRouter()


@cbv(metadata_router)
class MetadataController:
    """Controller for metadata"""

    @metadata_router.post("/ridecare/signals/video", response_model=ResponseMessage)
    async def process_video_signals(self, device_video_signals: VideoSignalsData,
                                    mongo_service: MongoService = Depends(lambda: di[MongoService]),
                                    voxel_service: VoxelService = Depends(lambda: di[VoxelService])):
        """
        Process device video signals

        Args:
            device_video_signals (VideoSignalsData): _description_
        """
        _logger.info("Processing video signals")
        await mongo_service.load_device_video_signals_data(device_video_signals)
        voxel_service.load_device_video_aggregated_metadata(device_video_signals)
        _logger.info("Signals has been processed successfully")

        return ResponseMessage()

    @metadata_router.post("/ridecare/signals/snapshot", response_model=ResponseMessage)
    async def process_snapshots_signals(self, device_snapshot_signals: SnapshotSignalsData,
                                        voxel_service: VoxelService = Depends(lambda: di[VoxelService])):
        """
        Process device snapshot signals

        Args:
            device_snapshot_signals (SnapshotSignalsData): _description_

        """
        voxel_service.load_snapshot_metadata(device_snapshot_signals)
        return ResponseMessage()

    @metadata_router.post("/ridecare/imu/video", response_model=ResponseMessage)
    async def process_video_imu(self, imu_data_artifact: IMUDataArtifact,
                                mongo_service: MongoService = Depends(lambda: di[MongoService])):
        """
        Process video IMU

        Args:
            imu_data_artifact (IMUDataArtifact): _description_
        """
        await mongo_service.process_imu_artifact(imu_data_artifact)
        return ResponseMessage()
