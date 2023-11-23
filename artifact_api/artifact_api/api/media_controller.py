"""Router for Media"""
from kink import di
from fastapi import APIRouter
from fastapi import Depends
from fastapi_restful.cbv import cbv
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from artifact_api.models import ResponseMessage
from artifact_api.mongo_controller import MongoController
from artifact_api.voxel import VoxelService


media_router = APIRouter()


@cbv(media_router)
class MediaController:

    """Controller for media"""
    @media_router.post("/ridecare/video", response_model=ResponseMessage)
    async def process_video_artifact(self, video_artifact: S3VideoArtifact,
                                     mongo_service: MongoController = Depends(lambda: di[MongoController]),
                                     voxel_service: VoxelService = Depends(lambda: di[VoxelService])
                                     ):
        """
        Process video artifact

        Args:
            video_artifact (S3VideoArtifact): _description_
            mongo_service (MongoController): _description_
            voxel_service (VoxelService): _description_
        """
        # Mongo service

        correlated_raw_paths = await mongo_service.get_correlated_snapshots_for_video(video_artifact)
        video_artifact.correlated_artifacts = correlated_raw_paths

        await mongo_service.create_video(video_artifact)
        await mongo_service.update_snapshots_correlations(video_artifact.correlated_artifacts,
                                                          video_artifact.artifact_id)

        # Voxel service
        voxel_service.update_voxel_video_correlated_snapshots(
            correlated=video_artifact.correlated_artifacts,
            artifact_id=video_artifact.artifact_id,
            tenant_id=video_artifact.tenant_id)
        voxel_service.create_voxel_video(artifact=video_artifact)
        return ResponseMessage()

    @media_router.post("/ridecare/snapshots", response_model=ResponseMessage)
    async def process_snapshot_artifact(self, snapshot_artifact: SnapshotArtifact,
                                        mongo_service: MongoController = Depends(lambda: di[MongoController]),
                                        voxel_service: VoxelService = Depends(lambda: di[VoxelService])):

        """
        Process snapshot artifact

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
            mongo_service (MongoController): _description_
            voxel_service (VoxelService): _description_
        """

        # Mongo service

        correlated_raw_paths = await mongo_service.get_correlated_videos_for_snapshot(snapshot_artifact)
        snapshot_artifact.correlated_artifacts = correlated_raw_paths

        await mongo_service.create_snapshot(snapshot_artifact)
        await mongo_service.update_videos_correlations(snapshot_artifact.correlated_artifacts,
                                                       snapshot_artifact.artifact_id)

        # Voxel service
        voxel_service.update_voxel_video_correlated_snapshots(
            correlated=snapshot_artifact.correlated_artifacts,
            artifact_id=snapshot_artifact.artifact_id,
            tenant_id=snapshot_artifact.tenant_id)
        voxel_service.create_voxel_snapshot(snapshot_artifact)
        return ResponseMessage()
