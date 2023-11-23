""" Voxel Service Implementation (abstract voxel operations). """
from typing import List
from kink import inject
from base.voxel.functions import create_dataset
from base.voxel.utils import determine_dataset_name
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from artifact_api.voxel.voxel_config import VoxelConfig
from artifact_api.voxel.voxel_snapshot import VoxelSnapshot
from artifact_api.voxel.voxel_video import VoxelVideo

@inject
class VoxelService:
    """
    Class responsible for managing the create and update samples
    """

    def __init__(self, voxel_config: VoxelConfig):
        self.voxel_config = voxel_config

    def create_voxel_video(self, artifact: S3VideoArtifact) -> None:
        """
        Updates or creates a sample in a dataset with the given metadata.
        If the sample or dataset do not exist they will be created.
        The dataset name is derived by the given S3 file path.
        """
        dataset_name, tags = determine_dataset_name(tenant=artifact.tenant_id,
                                                    is_snapshot=False,
                                                    mapping_config=self.voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelVideo.video_sample(artifact, dataset)

    def create_voxel_snapshot(self, artifact: SnapshotArtifact) -> None:
        """
        Updates or creates a sample in a dataset with the given metadata.
        If the sample or dataset do not exist they will be created.
        The dataset name is derived by the given S3 file path.
        """
        dataset_name, tags = determine_dataset_name(
            tenant=artifact.tenant_id,
            is_snapshot=True,
            mapping_config=self.voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelSnapshot.snapshot_sample(artifact, dataset)

    def update_voxel_video_correlated_snapshots(self, raw_correlated_filepaths: List[str],
                                                raw_filepath: str, tenant_id: str) -> None:
        """
        Update a video sample with correlated snapshots when a new snapshot arrives
        """
        dataset_name, _ = determine_dataset_name(tenant=tenant_id,
                                                 is_snapshot=False,
                                                 mapping_config=self.voxel_config.dataset_mapping)
        VoxelVideo.updates_correlation(raw_correlated_filepaths, raw_filepath, dataset_name)

    def update_voxel_snapshots_correlated_videos(self, raw_correlated_filepaths: List[str],
                                                 raw_filepath: str, tenant_id: str) -> None:
        """
        Update a snapshot sample with correlated videos when a new video arrives
        """
        dataset_name, _ = determine_dataset_name(tenant=tenant_id,
                                                 is_snapshot=True,
                                                 mapping_config=self.voxel_config.dataset_mapping)
        VoxelSnapshot.updates_correlation(raw_correlated_filepaths, raw_filepath, dataset_name)
