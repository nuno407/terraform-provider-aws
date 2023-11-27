""" Voxel Snapshot Model """
import fiftyone as fo
from base.model.artifacts import SnapshotArtifact
from artifact_api.voxel.voxel_base_models import VoxelSample, VoxelField


# pylint: disable=duplicate-code
class VoxelSnapshot(VoxelSample):  # pylint: disable=too-few-public-methods
    """
    Class responsible for parsing the message that comes from the API to populate snapshots
    """
    # Please add here all necessary fields for voxel
    fields = [
        VoxelField(field_name="video_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.artifact_id),
        VoxelField(field_name="tenant_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.tenant_id),
        VoxelField(field_name="device_id",
                   field_type=fo.core.fields.StringField,
                   field_value=lambda ctx: ctx.artifact.device_id),
        VoxelField(field_name="recording_time",
                   field_type=fo.core.fields.DateTimeField,
                   field_value=lambda ctx: ctx.artifact.timestamp),
        VoxelField(field_name="source_videos",
                   field_type=fo.core.fields.ListField,
                   field_subtype=fo.core.fields.StringField,
                   field_value=VoxelSample._correlated_paths("source_videos"))
    ]

    @classmethod
    def snapshot_sample(cls, artifact: SnapshotArtifact, dataset: fo.Dataset,
                        correlated_raw_filepaths: list[str]) -> None:
        """
        Creates or updates a snapshot sample

        Args:
            artifact (SnapshotArtifact): _description_
            dataset (fo.Dataset): _description_
        """
        cls._create_sample(artifact, dataset, correlated_raw_filepaths)

    @classmethod
    def updates_correlation(cls, raw_correlated_filepath: list[str], raw_filepath: str, dataset_name: str) -> None:
        """
        Updates all snapshots that have a correlation with this video
        """
        anonymized_filepath = VoxelSample._get_anonymized_path_from_raw(raw_filepath)
        anonymized_correlated = [cls._get_anonymized_path_from_raw(raw_path) for raw_path in raw_correlated_filepath]
        cls._update_correlation(anonymized_correlated, anonymized_filepath,
                                dataset_name, correlation_field="source_videos")
