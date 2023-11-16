"""Router for Media"""
from kink import di
from fastapi import APIRouter
from fastapi import Depends
from fastapi_restful.cbv import cbv
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from artifact_api.models import ResponseMessage
from artifact_api.mongo_controller import MongoController


media_router = APIRouter()


@cbv(media_router)
class MediaController:

    """Controller for media"""
    @media_router.post("/ridecare/video", response_model=ResponseMessage)
    async def process_video_artifact(self, video_artifact: S3VideoArtifact,
                                     mongo_service: MongoController = Depends(lambda: di[MongoController])):  # pylint: disable=unused-argument
        """
        Process video artifact

        Args:
            video_artifact (S3VideoArtifact): _description_
        """
        correlated_artifacts = await mongo_service.get_correlated_snapshots_for_video(video_artifact)
        video_artifact.correlated = [correlated_artifact async for correlated_artifact in correlated_artifacts]
        await mongo_service.create_video(video_artifact)
        await mongo_service.update_snapshots_correlations(video_artifact.correlated, video_artifact.artifact_id)
        return ResponseMessage()

    @media_router.post("/ridecare/snapshots", response_model=ResponseMessage)
    async def process_snapshot_artifact(self, snapshot_artifact: SnapshotArtifact,
                                        mongo_service: MongoController = Depends(lambda: di[MongoController])):  # pylint: disable=unused-argument

        """
        Process snapshot artifact

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
        """
        correlated_artifacts = await mongo_service.get_correlated_videos_for_snapshot(snapshot_artifact)
        snapshot_artifact.correlated = [correlated_artifact async for correlated_artifact in correlated_artifacts]
        await mongo_service.create_snapshot(snapshot_artifact)
        await mongo_service.update_videos_correlations(snapshot_artifact.correlated, snapshot_artifact.artifact_id)
        return ResponseMessage()
