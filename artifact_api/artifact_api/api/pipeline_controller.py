"""Router for pipeline outputs/updates"""
from datetime import datetime
from kink import di
import pytz
from fastapi import APIRouter
from fastapi import Depends
from fastapi_restful.cbv import cbv
from base.model.artifacts.api_messages import CHCDataResult
from base.model.artifacts import AnonymizationResult, PipelineProcessingStatus, PayloadType
from artifact_api.models import ResponseMessage
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.voxel import VoxelService

pipeline_router = APIRouter()


@cbv(pipeline_router)
class PipelineController:
    """Controller for pipeline outputs/updates"""

    @pipeline_router.post("/ridecare/pipeline/anonymize/video", response_model=ResponseMessage)
    async def process_anonymized_video(self, video_anon_result: AnonymizationResult):  # pylint: disable=unused-argument
        """
        Process anonymized video

        Args:
            video_anon_result (AnonymizationResult): _description_
        """
        return ResponseMessage()

    @pipeline_router.post("/ridecare/pipeline/anonymize/snapshot", response_model=ResponseMessage)
    async def create_anonymized_snapshot(self, snap_anon_result: AnonymizationResult):  # pylint: disable=unused-argument
        """
        Process anonymized snapshot

        Args:
            snap_anon_result (AnonymizationResult): _description_

        Returns:
            _type_: _description_
        """

        return ResponseMessage()

    @pipeline_router.post("/ridecare/pipeline/chc/video", response_model=ResponseMessage)
    async def create_video_chc_result(self, chc_result: CHCDataResult):  # pylint: disable=unused-argument
        """
        Process CHC result

        Args:
            chc_result (CHCDataResult): _description_
        """
        return ResponseMessage()

    @pipeline_router.post("/ridecare/pipeline/status", response_model=ResponseMessage)
    async def update_pipeline_status(self, pipeline_status: PipelineProcessingStatus,
                                     mongo_service: MongoService = Depends(
                                         lambda: di[MongoService]),
                                     voxel_service: VoxelService = Depends(lambda: di[VoxelService])):  # pylint: disable=unused-argument
        """
        Process pipelines status

        Args:
            pipeline_status (PipelineProcessingStatus): _description_

        """
        last_updated = datetime.now(tz=pytz.UTC)
        last_updated_str = str(last_updated.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        await mongo_service.create_pipeline_processing_status(pipeline_status, last_updated_str)

        if pipeline_status.object_type == PayloadType.VIDEO:
            voxel_service.attach_pipeline_processing_status_to_video(
                pipeline_status=pipeline_status, last_updated=last_updated)

        elif pipeline_status.object_type == PayloadType.SNAPSHOT:
            voxel_service.attach_pipeline_processing_status_to_snapshot(
                pipeline_status=pipeline_status, last_updated=last_updated)

        return ResponseMessage()
