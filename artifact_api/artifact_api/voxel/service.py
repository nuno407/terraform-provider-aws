""" Voxel Service Implementation (abstract voxel operations). """
from typing import List
from kink import inject
from base.voxel.functions import create_dataset
from base.voxel.utils import determine_dataset_name
from base.model.artifacts import S3VideoArtifact, SnapshotArtifact
from base.model.artifacts.upload_rule_model import SnapshotUploadRule, VideoUploadRule
from base.model.metadata.api_messages import SnapshotSignalsData
from artifact_api.voxel.voxel_config import VoxelConfig
from artifact_api.voxel.voxel_snapshot import VoxelSnapshot
from artifact_api.voxel.voxel_video import VoxelVideo
from artifact_api.voxel.voxel_metadata_transformer import VoxelMetadataTransformer

@inject
class VoxelService:
    """
    Class responsible for managing the create and update samples
    """

    def __init__(self, voxel_config: VoxelConfig, metadata_transformer: VoxelMetadataTransformer):
        self.__voxel_config = voxel_config
        self.__metadata_transformer = metadata_transformer

    def create_voxel_video(self, artifact: S3VideoArtifact, correlated_raw_filepaths: list[str] = []) -> None:  # pylint: disable=dangerous-default-value
        """
        Updates or creates a sample in a dataset with the given metadata.
        If the sample or dataset do not exist they will be created.
        The dataset name is derived by the given S3 file path.
        """
        dataset_name, tags = determine_dataset_name(tenant=artifact.tenant_id,
                                                    is_snapshot=False,
                                                    mapping_config=self.__voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelVideo.video_sample(artifact, dataset, correlated_raw_filepaths)

    def create_voxel_snapshot(self, artifact: SnapshotArtifact, correlated_raw_filepaths: list[str] = []) -> None:  # pylint: disable=dangerous-default-value
        """
        Updates or creates a sample in a dataset with the given metadata.
        If the sample or dataset do not exist they will be created.
        The dataset name is derived by the given S3 file path.
        """
        dataset_name, tags = determine_dataset_name(
            tenant=artifact.tenant_id,
            is_snapshot=True,
            mapping_config=self.__voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelSnapshot.snapshot_sample(artifact, dataset, correlated_raw_filepaths)

    def update_voxel_videos_with_correlated_snapshot(self, raw_correlated_filepaths: List[str],
                                                     raw_filepath: str, tenant_id: str) -> None:
        """
        Update a video sample with correlated snapshots when a new snapshot arrives
        """
        dataset_name, _ = determine_dataset_name(tenant=tenant_id,
                                                 is_snapshot=False,
                                                 mapping_config=self.__voxel_config.dataset_mapping)
        VoxelVideo.updates_correlation(raw_correlated_filepaths, raw_filepath, dataset_name)

    def update_voxel_snapshots_with_correlated_video(self, raw_correlated_filepaths: List[str],
                                                     raw_filepath: str, tenant_id: str) -> None:
        """
        Update a snapshot sample with correlated videos when a new video arrives
        """
        dataset_name, _ = determine_dataset_name(tenant=tenant_id,
                                                 is_snapshot=True,
                                                 mapping_config=self.__voxel_config.dataset_mapping)
        VoxelSnapshot.updates_correlation(raw_correlated_filepaths, raw_filepath, dataset_name)

    def attach_rule_to_video(self, rule: VideoUploadRule):
        """
        Attaches a upload rule to a video
        """
        dataset_name, tags = determine_dataset_name(
            tenant=rule.tenant,
            is_snapshot=False,
            mapping_config=self.__voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelVideo.attach_rule_to_video(dataset, rule)

    def attach_rule_to_snapshot(self, rule: SnapshotUploadRule):
        """
        Attaches a upload rule to a snapshot
        """
        dataset_name, tags = determine_dataset_name(
            tenant=rule.tenant,
            is_snapshot=True,
            mapping_config=self.__voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        VoxelSnapshot.attach_rule_to_snapshot(dataset, rule)

    def load_snapshot_metadata(self, signals_message: SnapshotSignalsData):
        """
        Add the metadata from the signals artifact to the sample
        """
        dataset_name, tags = determine_dataset_name(
            tenant=signals_message.message.tenant_id,
            is_snapshot=True,
            mapping_config=self.__voxel_config.dataset_mapping)
        dataset = create_dataset(dataset_name, tags)
        voxel_fields = self.__metadata_transformer.transform_snapshot_metadata_to_voxel(signals_message.data)
        VoxelSnapshot.load_metadata(dataset, signals_message.message, voxel_fields)