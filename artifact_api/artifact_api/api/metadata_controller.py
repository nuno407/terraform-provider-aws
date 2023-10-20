"""Router for metadata"""
from fastapi import APIRouter
from fastapi_restful.cbv import cbv
from base.model.metadata.api_messages import VideoSignalsData, SnapshotSignalsData, IMUDataArtifact
from artifact_api.models import ResponseMessage

metadata_router = APIRouter()


@cbv(metadata_router)
class MetadataController:
    """Controller for metadata"""
    @metadata_router.post("/ridecare/signals/video", response_model=ResponseMessage)
    async def process_video_signals(self, device_video_signals: VideoSignalsData):  # pylint: disable=unused-argument
        """
        Process device video signals

        Args:
            device_video_signals (VideoSignalsData): _description_
        """
        return {}

    @metadata_router.post("/ridecare/signals/snapshot", response_model=ResponseMessage)
    async def process_snapshots_signals(self, device_snapshot_signals: SnapshotSignalsData):  # pylint: disable=unused-argument
        """
        Process device snapshot signals

        Args:
            device_snapshot_signals (SnapshotSignalsData): _description_

        """
        return {}

    @metadata_router.post("/ridecare/imu/video", response_model=ResponseMessage)
    async def process_video_imu(self, device_video_imu: IMUDataArtifact):  # pylint: disable=unused-argument
        """
        Process video IMU

        Args:
            device_video_imu (IMUDataArtifact): _description_
        """
        return {}
