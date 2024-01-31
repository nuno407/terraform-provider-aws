"""Router for Media"""
import logging
from kink import di
from fastapi import APIRouter
from fastapi import Depends
from fastapi_restful.cbv import cbv

from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from artifact_api.models import ResponseMessage
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.voxel import VoxelService


media_router = APIRouter()
_logger = logging.getLogger(__name__)


@cbv(media_router)
class MediaController:

    """Controller for media"""
    @media_router.post("/ridecare/video", response_model=ResponseMessage)
    async def process_video_artifact(self, video_artifact: S3VideoArtifact,
                                     mongo_service: MongoService = Depends(lambda: di[MongoService]),
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
        _logger.info("Processing video artifact: %s", video_artifact.model_dump_json())
        correlated_db_artifacts = await mongo_service.get_correlated_snapshots_for_video(video_artifact)

        correlated_snapshot_ids = [db_artifact.video_id for db_artifact in correlated_db_artifacts]
        raw_file_paths = [
            db_artifact.filepath for db_artifact in correlated_db_artifacts if db_artifact.filepath is not None]

        _logger.debug("Got the following correlated snapshots: %s", str(correlated_snapshot_ids))

        await mongo_service.upsert_video(video_artifact, correlated_snapshot_ids)
        await mongo_service.update_snapshots_correlations(correlated_snapshot_ids,
                                                          video_artifact.artifact_id)

        # Voxel service
        voxel_service.update_voxel_snapshots_with_correlated_video(
            raw_correlated_filepaths=raw_file_paths,
            raw_filepath=video_artifact.s3_path,
            tenant_id=video_artifact.tenant_id)
        voxel_service.create_voxel_video(artifact=video_artifact, correlated_raw_filepaths=raw_file_paths)
        return ResponseMessage()

    @media_router.post("/ridecare/snapshots", response_model=ResponseMessage)
    async def process_snapshot_artifact(self, snapshot_artifact: SnapshotArtifact,
                                        mongo_service: MongoService = Depends(lambda: di[MongoService]),
                                        voxel_service: VoxelService = Depends(lambda: di[VoxelService])):

        """
        Process snapshot artifact

        Args:
            snapshot_artifact (SnapshotArtifact): _description_
            mongo_service (MongoController): _description_
            voxel_service (VoxelService): _description_
        """
        _logger.info("Processing snapshot artifact: %s", snapshot_artifact.model_dump_json())

        # Mongo service
        correlated_db_artifacts = await mongo_service.get_correlated_videos_for_snapshot(snapshot_artifact)

        correlated_video_ids = [db_artifact.video_id for db_artifact in correlated_db_artifacts]
        raw_file_paths = [
            db_artifact.filepath for db_artifact in correlated_db_artifacts if db_artifact.filepath is not None]

        _logger.debug("Got the following correlated videos: %s", str(correlated_video_ids))

        await mongo_service.upsert_snapshot(snapshot_artifact, correlated_video_ids)
        await mongo_service.update_videos_correlations(correlated_video_ids,
                                                       snapshot_artifact.artifact_id)

        # Voxel service
        voxel_service.update_voxel_videos_with_correlated_snapshot(
            raw_correlated_filepaths=raw_file_paths,
            raw_filepath=snapshot_artifact.s3_path,
            tenant_id=snapshot_artifact.tenant_id)
        voxel_service.create_voxel_snapshot(artifact=snapshot_artifact, correlated_raw_filepaths=raw_file_paths)
        return ResponseMessage()
