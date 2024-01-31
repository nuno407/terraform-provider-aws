"""Controller for Events"""
from fastapi import APIRouter, Depends
from fastapi_restful.cbv import cbv
from kink import di
from base.model.artifacts.upload_rule_model import SnapshotUploadRule, VideoUploadRule
from artifact_api.voxel.service import VoxelService
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.models import ResponseMessage


upload_rule_router = APIRouter()


@cbv(upload_rule_router)
class UploadRuleController:
    """Controller for events"""

    @upload_rule_router.post("/ridecare/upload_rule/video", response_model=ResponseMessage)
    async def process_device_event(self,
                                   message: VideoUploadRule,
                                   mongo_service: MongoService = Depends(lambda: di[MongoService]),
                                   voxel_service: VoxelService = Depends(lambda: di[VoxelService])
                                   ):
        """
        Process a upload_rule trigger for a video
        """
        await mongo_service.attach_rule_to_video(message)
        voxel_service.attach_rule_to_video(message)
        return ResponseMessage()

    @upload_rule_router.post("/ridecare/upload_rule/snapshot", response_model=ResponseMessage)
    async def process_operator_feedback(self,
                                        message: SnapshotUploadRule,
                                        mongo_service: MongoService = Depends(lambda: di[MongoService]),
                                        voxel_service: VoxelService = Depends(lambda: di[VoxelService])
                                        ):
        """
        Process a upload_rule trigger for a snapshot
        """
        await mongo_service.attach_rule_to_snapshot(message)
        voxel_service.attach_rule_to_snapshot(message)
        return ResponseMessage()
