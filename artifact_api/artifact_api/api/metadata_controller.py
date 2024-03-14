"""Router for metadata"""
import logging
from base64 import b64decode
from io import BytesIO
import pandas as pd
from kink import di
from fastapi import APIRouter, Depends
from fastapi_restful.cbv import cbv
from base.model.artifacts.api_messages import VideoSignalsData, SnapshotSignalsData, IMUDataArtifact, IMUProcessedData
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
        _logger.info("Processing video IMU")

        # Decode the base64 encoded parquet file
        buffer = BytesIO(b64decode(imu_data_artifact.data))
        del imu_data_artifact.data

        _logger.debug("IMU file has been decoded successfully")

        # Read the parquet file and validate the data
        df = pd.read_parquet(buffer, engine="fastparquet")
        df_typed = IMUProcessedData.validate(df)
        _logger.debug("IMU been read and validated successfully")

        await mongo_service.process_imu_artifact(imu_data_artifact.message.tenant_id, df_typed)

        _logger.info("Video IMU has been ingested successfully")
        return ResponseMessage()
