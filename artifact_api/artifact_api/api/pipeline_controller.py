"""Router for pipeline outputs/updates"""
from fastapi import APIRouter
from fastapi_restful.cbv import cbv
from base.model.metadata.api_messages import CHCDataResult
from base.model.artifacts import AnonymizationResult, PipelineProcessingStatus
from artifact_api.models import ResponseMessage

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
        return {}

    @pipeline_router.post("/ridecare/pipeline/anonymize/snapshot", response_model=ResponseMessage)
    async def create_anonymized_snapshot(self, snap_anon_result: AnonymizationResult):  # pylint: disable=unused-argument
        """
        Process anonymized snapshot

        Args:
            snap_anon_result (AnonymizationResult): _description_

        Returns:
            _type_: _description_
        """

        return {}

    @pipeline_router.post("/ridecare/pipeline/chc/video", response_model=ResponseMessage)
    async def create_video_chc_result(self, chc_result: CHCDataResult):  # pylint: disable=unused-argument
        """
        Process CHC result

        Args:
            chc_result (CHCDataResult): _description_
        """
        return {}

    @pipeline_router.post("/ridecare/pipeline/status", response_model=ResponseMessage)
    async def update_pipeline_status(self, pipeline_status: PipelineProcessingStatus):  # pylint: disable=unused-argument
        """
        Process pipelines status

        Args:
            pipeline_status (PipelineProcessingStatus): _description_

        """
        return {}
