"""Router for Media"""
from fastapi import APIRouter
from fastapi_restful.cbv import cbv
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from artifact_api.models import ResponseMessage

media_router = APIRouter()


@cbv(media_router)
class MediaController:
    """Controller for media"""
    @media_router.post("/ridecare/video", response_model=ResponseMessage)
    async def process_video_artifact(self, video_artifact: S3VideoArtifact):  # pylint: disable=unused-argument
        """
        Process video artifact

        Args:
            video_artifact (S3VideoArtifact): _description_
        """
        return {}

    @media_router.post("/ridecare/snapshots", response_model=ResponseMessage)
    async def process_snapshot_artifact(self, snapshot_artifact: SnapshotArtifact):  # pylint: disable=unused-argument
        """
        Process snapshot artifact

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
        """
        return {}
